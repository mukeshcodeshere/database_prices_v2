from mvconnectivity import MvWSConnection
import datetime
from dotenv import load_dotenv
import os
load_dotenv()
GvWSUSERNAME = os.getenv("GvWSUSERNAME")
GvWSPASSWORD = os.getenv("GvWSPASSWORD")

server_connection = MvWSConnection(GvWSUSERNAME, GvWSPASSWORD)
example_environment = "onboard"

# Sample request

mv_result = server_connection.get_instrument_list(
    symbol = "#EIA",
    env = example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)

print(mv_result.dtypes)
mv_result.to_csv("mv_result.csv")

