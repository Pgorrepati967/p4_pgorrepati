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

class LenderServicer(lender_pb2_grpc.LenderServicer):
    def __init__(self):
        hdfs_host = os.environ.get("HDFS_HOST", "nn")
        hdfs_port = int(os.environ.get("HDFS_PORT", "9000"))
        self.hdfs = fs.HadoopFileSystem(hdfs_host, port=hdfs_port)

        self.mysql_user = os.environ.get("MYSQL_USER", "root")
        self.mysql_pass = os.environ.get("MYSQL_PASSWORD", "acid")
        self.mysql_host = os.environ.get("MYSQL_HOST", "p4-mysql-1:3306")
        self.mysql_db = os.environ.get("MYSQL_DATABASE", "CS544")

        os.environ["CLASSPATH"] = subprocess.check_output(
            ["hadoop", "classpath", "--glob"], text=True
        ).strip()

    def _connect_mysql(self):
        """Connect to MySQL with retry loop."""
        engine = None
        for _ in range(10):
            try:
                engine = create_engine(
                    f"mysql+pymysql://{self.mysql_user}:{self.mysql_pass}@{self.mysql_host}/{self.mysql_db}"
                )
                conn = engine.connect()
                conn.close()
                return engine
            except OperationalError:
                time.sleep(3)
        raise RuntimeError("Could not connect to MySQL after multiple retries")

    def DbToHdfs(self, request, context):
        """Query MySQL and upload full Parquet to HDFS."""
        try:
            engine = self._connect_mysql()
            query = """
            SELECT loans.*
            FROM loans
            WHERE loans.loan_type='Conventional'
            """
            df = pd.read_sql(query, engine)
            table = pa.Table.from_pandas(df)

            parquet_path = os.environ.get("PARQUET_PATH", "/hdma-wi-2021.parquet")
            block_size = int(os.environ.get("BLOCK_SIZE", 1024*1024)) 

            pq.write_table(
                table,
                parquet_path,
                filesystem=self.hdfs,
                version="2.0",
                compression="snappy",
                row_group_size=block_size
            )
            return lender_pb2.DbToHdfsResp(row_count=df.shape[0], error="")
        except Exception as e:
            return lender_pb2.DbToHdfsResp(row_count=0, error=str(e))

    def BlockLocations(self, request, context):
        """Return block locations as a dict."""
        path = request.path
        try:
            output = subprocess.check_output(
                ["hdfs", "fsck", path, "-files", "-blocks", "-locations"], text=True
            )
            blocks = {}
            for line in output.splitlines():
                if "BP-" in line:
                    node_id = line.split()[-1].strip()
                    blocks[node_id] = blocks.get(node_id, 0) + 1
            return lender_pb2.BlockLocationsResp(block_entries=blocks, error="")
        except Exception as e:
            return lender_pb2.BlockLocationsResp(block_entries={}, error=str(e))

    def CalcAvgLoan(self, request, context):
        """Compute county average with caching and fault tolerance."""
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
                table = pq.read_table(big_file, filesystem=self.hdfs,
                                      filters=[("county_code", "=", county_code)])
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
