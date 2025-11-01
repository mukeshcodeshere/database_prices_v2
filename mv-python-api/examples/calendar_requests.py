from mvconnectivity import MvWSConnection
import datetime
import os
from dotenv import load_dotenv
load_dotenv()
GvWSUSERNAME = os.getenv("GvWSUSERNAME")
GvWSPASSWORD = os.getenv("GvWSPASSWORD")

server_connection = MvWSConnection(GvWSUSERNAME, GvWSPASSWORD)
example_environment = "onboard"

# Sample request | All rollover calendars

mv_result = server_connection.get_calendar(
    calendartype = "Expiry",
    env = example_environment
)    
mv_result = mv_result.to_dataframe()
print(mv_result)

# Sample request | HENG (holiday) & RBOMBHENG (rollover) calendars

mv_result = server_connection.get_calendar(
    calendar = ["HENG", "RBOMBHENG"],
    env = example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)

# Sample request | 2024 HUSA calendar

mv_result = server_connection.get_calendar_details(
    calendar = 'HUSA',
    quoting = False,
    startdate = datetime.date(2025, 1, 1),
    enddate = datetime.date(2025, 12, 31),
    env = example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)

# Sample request | 2024 RBOMBHENG rollover calendar expiry dates

mv_result = server_connection.get_calendar_details(
    calendar = 'RBOMBHENG',
    quoting = False,
    startdate = datetime.date(2025, 1, 1),
    enddate = datetime.date(2025, 12, 31),
    env = example_environment
)   
mv_result = mv_result.to_dataframe()
print(mv_result)

# Sample request | 2024 RBOMBHENG expiry dates with tenors

mv_result = server_connection.get_calendar_details(
    calendar = 'REOMBALT',
    quoting = False, 
    startdate = datetime.date(2025, 1, 1), 
    enddate = datetime.date(2025, 12, 31),
    tenor = ["M01", "M02", "2025Q03", "2025M06"],
    env = example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)

# Sample request | REOMBALT rollover calendar with selected tenors

mv_result = server_connection.get_contract_details(
    rollover = "REOMBALT",
    tenor = ["M01", "M02"],
    startdate = datetime.date(2025, 1, 1),
    enddate = datetime.date(2025, 12, 31),
    env = example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)

# Sample request | REOMBALT rollover calendar with selected tenors & onDate

mv_result = server_connection.get_contract_details(
    rollover = "REOMBALT",
    tenor = ["M01", "M02"],
    startdate = datetime.date(2025, 1, 1),
    enddate = datetime.date(2025, 12, 31),
    ondate = datetime.date(2025, 6, 21),
    env = example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)
