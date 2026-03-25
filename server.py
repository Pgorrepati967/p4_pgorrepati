import grpc
from concurrent import futures
import lender_pb2, lender_pb2_grpc
import traceback
import time
from sqlalchemy import create_engine, text
class LenderServicer(lender_pb2_grpc.LenderServicer):
    def DbToHdfs(self, request, context):
        db_url = "mysql+pymysql://root:abc@p4-mysql-1:3306/CS544"
        engine = create_engine(db_url)
        start_time = time.time()
        timeout = 30
        while time.time() - start_time < timeout:
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT COUNT(*) FROM input"))
                print("Connected to MySQL, row_count =", row_count)
                break
            except Exception as e:
                print("MySQL not ready, retrying...", e)
                time.sleep(2)
        else:
            return lender_pb2.DbToHdfsResp(
                row_count=0;
                error="Could not connect to MySQL after retries"
            )
        return lender_pb2.DbToHdfsResp(
            row_count=row_count,
            error="not implemented"
        )
    def BlockLocations(self, request, context):
        return lender_pb2.BlockLocationsResp(error="not implemented")

    def CalcAvgLoan(self, request, context):
        return lender_pb2.CalcAvgLoanResp(error="not implemented")

def main():
    print("start server")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8), options=[("grpc.so_reuseport", 0)])
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port("0.0.0.0:5000")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    main()
