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
        return lender_pb2.BlockLocationsResp(error="not implemented")

    def CalcAvgLoan(self, request, context):
        return lender_pb2.CalcAvgLoanResp(error="not implemented")


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
