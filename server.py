import grpc
from concurrent import futures
import lender_pb2, lender_pb2_grpc
import time
from sqlalchemy import create_engine, text
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
import os, subprocess
import requests
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calc_avg_loan(county_code: str) -> dict:
    county = int(float(county_code))
    hdfs = fs.HadoopFileSystem(host="nn", port=9000, user="hduser")
    partition_path = f"/partitions/{county}.parquet"
    source = ""
    table = None

    try:
        table = pq.read_table(partition_path, filesystem=hdfs)
        source = "reuse"
        logging.info(f"Cache hit for county {county}. Reusing existing partition.")
    except FileNotFoundError:
        source = "create"
        logging.info(f"Cache miss for county {county}. Creating new partition.")
    except OSError as e:
        source = "recreate"
        logging.warning(f"Cache for county {county} is corrupted or unavailable (OSError: {e}). Recreating.")

    if source in ["create", "recreate"]:
        table = pq.read_table(
            "/hdma-wi-2021.parquet",
            filesystem=hdfs,
            filters=[("county_code", "=", county)]
        )
        try:
            hdfs.create_dir("/partitions")
        except pa.ArrowIOError as e:
            if "File exists" not in str(e):
                raise
        pq.write_table(
            table,
            f"hdfs://nn:9000{partition_path}",
            filesystem=hdfs,
            compression="snappy"
        )

    df = table.to_pandas()
    if len(df) == 0:
        avg = 0
    else:
        avg = int(df["loan_amount"].mean())

    return {"avg": avg, "source": source}


class LenderServicer(lender_pb2_grpc.LenderServicer):

    def DbToHdfs(self, request, context):
        db_url = "mysql+pymysql://root:acid@p4-mysql-1:3306/CS544"
        engine = create_engine(db_url)

        start_time = time.time()
        timeout = 30

        while time.time() - start_time < timeout:
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                print("Connected to MySQL")
                break
            except Exception as e:
                print("MySQL not ready, retrying...", e)
                time.sleep(2)
        else:
            return lender_pb2.DbToHdfsResp(
                row_count=0,
                error="Could not connect to MySQL after retries"
            )

        try:
            query = """
                SELECT * 
                FROM loans 
                WHERE loan_type='Conventional'
            """

            df = pd.read_sql(query, engine)

            os.environ["CLASSPATH"] = subprocess.check_output(
                ["hadoop", "classpath", "--glob"], text=True
            ).strip()

            hdfs = fs.HadoopFileSystem(host="nn", port=9000, user="hduser")

            table = pa.Table.from_pandas(df)

            pq.write_table(
                table,
                "hdfs://nn:9000/hdma-wi-2021.parquet",
                filesystem=hdfs,
                compression="snappy",
                use_dictionary=True,
                row_group_size=1024 * 1024,
            )

            return lender_pb2.DbToHdfsResp(
                row_count=len(df),
                error=""
            )

        except Exception as e:
            return lender_pb2.DbToHdfsResp(
                row_count=0,
                error=str(e)
            )

    def BlockLocations(self, request, context):
        path = request.path

        try:
            url = f"http://nn:9870/webhdfs/v1{path}?op=GETFILEBLOCKLOCATIONS"
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()

            counts = defaultdict(int)
            blocks = data.get("BlockLocations", {}).get("BlockLocation", [])

            for block in blocks:
                for host in block.get("hosts", []):
                    counts[host] += 1

            return lender_pb2.BlockLocationsResp(
                block_entries=dict(counts),
                error=""
            )

        except Exception as e:
            return lender_pb2.BlockLocationsResp(
                block_entries={},
                error=str(e)
            )
    def CalcAvgLoan(self, request, context):
        try:
            result = calc_avg_loan(request.county_code)
            return lender_pb2.CalcAvgLoanResp(
                avg_loan=result["avg"],
                source=result["source"],
                error=""
            )
        except Exception as e:
            return lender_pb2.CalcAvgLoanResp(
                avg_loan=0,
                source="",
                error=str(e)
            )

def main():
    print("start server")
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8),
        options=[("grpc.so_reuseport", 0)]
    )
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port("0.0.0.0:5000")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
