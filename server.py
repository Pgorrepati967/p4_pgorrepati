import grpc
from concurrent import futures
import lender_pb2, lender_pb2_grpc
import os
import subprocess
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from collections import defaultdict

class LenderServicer(lender_pb2_grpc.LenderServicer):
    def __init__(self):
        os.environ["CLASSPATH"] = subprocess.check_output(
            ["hadoop", "classpath", "--glob"], text=True
        ).strip()

        hdfs_host = os.environ.get("HDFS_HOST", "nn")
        hdfs_port = int(os.environ.get("HDFS_PORT", "9000"))

        self.hdfs = None
        for _ in range(10):
            try:
                self.hdfs = fs.HadoopFileSystem(hdfs_host, port=hdfs_port,replication=2,default_block_size=1024 * 1024)
                break
            except OSError as e:
                print(f"HDFS connection failed: {e}, retrying...")
                time.sleep(3)
        if self.hdfs is None:
            raise RuntimeError("Could not connect to HDFS after multiple retries")

        self.mysql_user = os.environ.get("MYSQL_USER", "root")
        self.mysql_pass = os.environ.get("MYSQL_PASSWORD", "acid")
        self.mysql_host = os.environ.get("MYSQL_HOST","mysql")
        self.mysql_db = os.environ.get("MYSQL_DATABASE", "CS544")

    def _connect_mysql(self):
        engine = None
        for _ in range(10):
            try:
                engine = create_engine(
                    f"mysql+pymysql://{self.mysql_user}:{self.mysql_pass}@{self.mysql_host}:3306/{self.mysql_db}"
                )
                conn = engine.connect()
                conn.close()
                return engine
            except OperationalError as e:
                print(f"MySQL connection failed: {e}, retrying...")
                time.sleep(3)
        raise RuntimeError("Could not connect to MySQL after multiple retries")

    def DbToHdfs(self, request, context):
        try:
            engine = self._connect_mysql()
            query = """
            SELECT l.*
            FROM loans l
            INNER JOIN loan_types t
                ON l.loan_type_id = t.id
            WHERE t.loan_type_name = 'conventional';
            """
            df = pd.read_sql(query, engine)
            table = pa.Table.from_pandas(df)

            parquet_path = os.environ.get("PARQUET_PATH", "/hdma-wi-2021.parquet")

            parent_dir = os.path.dirname(parquet_path)
            if parent_dir:
                try:
                    info = self.hdfs.get_file_info(parent_dir)
                    if info.type != fs.FileType.Directory:
                        return lender_pb2.DbToHdfsResp(row_count=0, error=f"HDFS path {parent_dir} is not a directory")
                except FileNotFoundError:
                    return lender_pb2.DbToHdfsResp(row_count=0, error=f"HDFS directory {parent_dir} not found")

            pq.write_table(
                table,
                parquet_path,
                filesystem=self.hdfs,
                compression="snappy",
            )
            return lender_pb2.DbToHdfsResp(row_count=df.shape[0], error="")
        except Exception as e:
            return lender_pb2.DbToHdfsResp(row_count=0, error=str(e))

    def BlockLocations(self, request, context):
        raw_path = request.path or os.environ.get("PARQUET_PATH", "/hdma-wi-2021.parquet")
        if raw_path.startswith("hdfs://"):
            raw_path = "/" + "/".join(raw_path.split("/")[3:])

        try:
            output = subprocess.check_output(
                ["hdfs", "fsck", raw_path, "-files", "-blocks", "-locations"],
                text=True
            )

            node_counts = defaultdict(int)
            for line in output.splitlines():
                if "DatanodeInfoWithStorage[" in line:
                    node_id = line.split(":")[0].split("[")[-1]
                    node_counts[node_id] += 1

            return lender_pb2.BlockLocationsResp(block_entries=dict(node_counts), error="")

        except FileNotFoundError:
            return lender_pb2.BlockLocationsResp(block_entries={}, error=f"File {raw_path} not found on HDFS")
        except Exception as e:
            return lender_pb2.BlockLocationsResp(block_entries={}, error=str(e))

    def CalcAvgLoan(self, request, context):
        county_code = float(request.county_code)
        big_file = os.environ.get("PARQUET_PATH", "/hdma-wi-2021.parquet")
        partition_dir = os.environ.get("PARTITION_DIR", "/partitions")
        os.makedirs(partition_dir, exist_ok=True)
        county_file = f"{partition_dir}/{int(county_code)}.parquet"

        source = "create"
        df = None

        try:
            df = pq.read_table(county_file, filesystem=self.hdfs).to_pandas()
            source = "reuse"
        except (FileNotFoundError, OSError):
            try:
                table = pq.read_table(
                    big_file,
                    filesystem=self.hdfs,
                    filters=[("county_code", "=", county_code)]
                )
                df = table.to_pandas()
                pq.write_table(table, county_file, filesystem=self.hdfs)
                if source != "reuse":
                    source = "recreate"
            except Exception as e:
                return lender_pb2.CalcAvgLoanResp(avg_loan=0, source="", error=str(e))

        avg_loan = int(df["loan_amount"].mean())
        return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source, error="")

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port("[::]:5000")
    server.start()
    print("Server running on port 5000...")
    server.wait_for_termination()

if __name__ == "__main__":
    main()
