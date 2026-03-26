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
        os.environ["CLASSPATH"] = subprocess.check_output(
            ["hadoop", "classpath", "--glob"], text=True
        ).strip()

        self.hdfs = None
        for _ in range(10):
            try:
                self.hdfs = fs.HadoopFileSystem("nn", port=9000, replication=2, default_block_size=1024 * 1024)
                break
            except OSError:
                time.sleep(3)
        if self.hdfs is None:
            raise RuntimeError("Could not connect to HDFS")

        self.mysql_user = "root"
        self.mysql_pass = "acid"
        self.mysql_host = "mysql"
        self.mysql_db = "CS544"

    def _connect_mysql(self):
        for _ in range(10):
            try:
                engine = create_engine(
                    f"mysql+pymysql://{self.mysql_user}:{self.mysql_pass}@{self.mysql_host}:3306/{self.mysql_db}"
                )
                conn = engine.connect()
                conn.close()
                return engine
            except OperationalError:
                time.sleep(3)
        raise RuntimeError("Could not connect to MySQL")

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

            path = "/hdma-wi-2021.parquet"

            with self.hdfs.open_output_stream(path) as stream:
                pq.write_table(table, stream, compression="snappy")

            return lender_pb2.DbToHdfsResp(row_count=df.shape[0], error="")
        except Exception as e:
            return lender_pb2.DbToHdfsResp(row_count=0, error=str(e))

    def BlockLocations(self, request, context):
        try:
            block_locations = self.hdfs.get_file_block_locations(request.path)
            blocks = {}
            for block in block_locations:
                for host in block['names']:
                    host_str = host.decode('utf-8')
                    blocks[host_str] = blocks.get(host_str, 0) + 1

            return lender_pb2.BlockLocationsResp(block_entries=blocks, error="")
        except Exception as e:
            return lender_pb2.BlockLocationsResp(block_entries={}, error=str(e))

    def CalcAvgLoan(self, request, context):
        try:
            subprocess.check_call(["hdfs", "dfs", "-mkdir", "-p", "/partitions"])
        except:
            pass
        county_code = float(request.county_code)
        big_file = "/hdma-wi-2021.parquet"
        partition_dir = "/partitions"
        county_file = f"{partition_dir}/{int(county_code)}.parquet"

        try:
            df = pq.read_table(county_file, filesystem=self.hdfs).to_pandas()
            source = "reuse"
        except (FileNotFoundError, OSError) as e:
            if isinstance(e, FileNotFoundError):
                source = "create"
            else:
                source = "recreate"

            table = pq.read_table(
                big_file,
                filesystem=self.hdfs,
                filters=[("county_code", "=", county_code)]
            )
            df = table.to_pandas()

            hdfs_rep1 = fs.HadoopFileSystem("nn", port=9000, replication=1)
            with hdfs_rep1.open_output_stream(county_file) as stream:
                pq.write_table(table, stream, compression="snappy")

        if df.empty or df["loan_amount"].isnull().all():
            avg_loan = 0
        else:
            avg_loan = int(df["loan_amount"].mean())
        return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source, error="")

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port("[::]:5000")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    main()

