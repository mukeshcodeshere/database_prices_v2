from mvconnectivity import MvWSConnection
import datetime
import os
from dotenv import load_dotenv

load_dotenv()
GvWSUSERNAME = os.getenv("GvWSUSERNAME")
GvWSPASSWORD = os.getenv("GvWSPASSWORD")

server_connection = MvWSConnection(GvWSUSERNAME, GvWSPASSWORD)
example_environment = "onboard"

# Sample GetExchangeList request

mv_result = server_connection.get_exchange_list(env = example_environment)
mv_result = mv_result.to_dataframe()
print(mv_result)
#v_result.to_csv("exchange.csv")
# Sample GetPermissionedRoots request

fields = ["description", "optionroot", "symbol"]
mv_result = server_connection.get_permissioned_roots(
	fields = fields,
	env = example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)
#mv_result.to_csv("roots.csv")
