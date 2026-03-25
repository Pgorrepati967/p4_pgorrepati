import grpc
from concurrent import futures
import lender_pb2, lender_pb2_grpc
import traceback

class LenderServicer(lender_pb2_grpc.LenderServicer):
    def DbToHdfs(self, request, context):
        db_url = f"mysql+pymysql://root:abc@127.0.0.1:3306/cs544"

        engine = None
        start_time = time.time()
        timeout = 30
        while time.time() - start_time < timeout:
            try:
                engine = create_engine(db_url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                print("Connected to MySQL")
                break
            except Exception as e:
                print("MySQL not ready, retrying...", e)
                time.sleep(2)
        if engine is None:
            return lender_pb2.DbToHdfsResp(
                error="Could not connect to MySQL after retries"
            )
        return lender_pb2.DbToHdfsResp(
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
