from mvconnectivity import MvWSConnection
from dotenv import load_dotenv
import os
load_dotenv()
GvWSUSERNAME = os.getenv("GvWSUSERNAME")
GvWSPASSWORD = os.getenv("GvWSPASSWORD")

server_connection = MvWSConnection(GvWSUSERNAME, GvWSPASSWORD)
example_environment = "onboard"

# Sample request

# options_fields = ["atmindex", "callput", "symbol", "strike", "mrv", "impvol", "close"]
# mv_result = server_connection.get_option_analytics(
#     fields = options_fields,
#     underlier = "/GCLM25",
#     optionmodel = "bs",
#     strikecount = 10,
#     env = example_environment
# )
# mv_result = mv_result.to_dataframe()
# print(mv_result)

# Sample request

options_single_field = ["symbol", "strike", "impvol", "delta", "ghamma", "rho", "theta", "vega"]
mv_result = server_connection.get_option_analytics_single(
    fields = options_single_field,
    symbols = "-GCLM25C15000",
    optionmodel = "bs",
    env = example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)