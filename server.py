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

            pq.write_table(table, path, filesystem=self.hdfs, compression="snappy")

            return lender_pb2.DbToHdfsResp(row_count=df.shape[0], error="")
        except Exception as e:
            return lender_pb2.DbToHdfsResp(row_count=0, error=str(e))

    def BlockLocations(self, request, context):
        try:
            output = subprocess.check_output(
                ["hdfs", "fsck", request.path, "-files", "-blocks", "-locations"],
                text=True
            )

            blocks = {}

            for line in output.splitlines():
                if "DatanodeInfoWithStorage" in line:
                    parts = line.split("[")[1:]
                    for part in parts:
                        node = part.split(":")[0]
                        blocks[node] = blocks.get(node, 0) + 1

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
        except FileNotFoundError:
            table = pq.read_table(
                big_file,
                filesystem=self.hdfs,
                filters=[("county_code", "=", county_code)]
            )
            df = table.to_pandas()

            tmp_local = f"/tmp/{int(county_code)}.parquet"
            pq.write_table(table, tmp_local, compression="snappy")

            subprocess.check_call([
                "hdfs", "dfs",
                "-D", "dfs.replication=1",
                "-put", "-f",
                tmp_local,
                county_file
            ])

            source = "create"

        except OSError:
            table = pq.read_table(
                big_file,
                filesystem=self.hdfs,
                filters=[("county_code", "=", county_code)]
            )
            df = table.to_pandas()

            tmp_local = f"/tmp/{int(county_code)}.parquet"
            pq.write_table(table, tmp_local, compression="snappy")

            subprocess.check_call([
                "hdfs", "dfs",
                "-D", "dfs.replication=1",
                "-put", "-f",
                tmp_local,
                county_file
            ])

            source = "recreate"

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

