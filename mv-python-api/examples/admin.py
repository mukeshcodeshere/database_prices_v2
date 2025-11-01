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
print("Getting exchange list...")
mv_result = server_connection.get_exchange_list(env=example_environment)
mv_result = mv_result.to_dataframe()
print(mv_result)
# mv_result.to_csv("exchange.csv")

# Sample GetPermissionedRoots request
print("\nGetting permissioned roots...")
fields = ["description", "optionroot", "symbol"]
mv_result = server_connection.get_permissioned_roots(
    fields=fields,
    env=example_environment
)
mv_result = mv_result.to_dataframe()
print(mv_result)
# mv_result.to_csv("roots.csv")

# Get all EIA symbols using different search methods
print("\n=== SEARCHING FOR EIA SYMBOLS ===")

# Method 1: Search in permissioned roots
print("\n1. EIA symbols found in permissioned roots:")
try:
    eia_symbols = mv_result[mv_result['symbol'].str.contains('EIA', na=False)]
    print(f"Found {len(eia_symbols)} EIA symbols in permissioned roots:")
    print(eia_symbols)
except Exception as e:
    print(f"Error filtering permissioned roots: {e}")

# Method 2: Use SymbolSearch endpoint
print("\n2. Searching for EIA symbols using SymbolSearch...")
try:
    search_result = server_connection.symbol_search(
        pattern="EIA",
        records=1000,  # Increase for more results
        user=GvWSUSERNAME,
        page=1,
        pageSize=100,
        env=example_environment
    )
    search_result = search_result.to_dataframe()
    print(f"Found {len(search_result)} symbols with 'EIA' pattern:")
    print(search_result[['symbol', 'description']].head(20))  # Show first 20
except Exception as e:
    print(f"Error with SymbolSearch: {e}")

# Method 3: Use SymbolSearchMV endpoint (MarketView specific)
print("\n3. Searching for EIA symbols using SymbolSearchMV...")
try:
    search_result_mv = server_connection.symbol_search_mv(
        pattern="EIA",
        records=1000,
        user=GvWSUSERNAME,
        page=1,
        pageSize=100,
        env=example_environment
    )
    search_result_mv = search_result_mv.to_dataframe()
    print(f"Found {len(search_result_mv)} symbols with 'EIA' pattern (MV):")
    print(search_result_mv[['symbol', 'description']].head(20))
except Exception as e:
    print(f"Error with SymbolSearchMV: {e}")

# Method 4: Use ExactSymbolSearch for specific EIA symbols
print("\n4. Exact symbol search for EIA patterns...")
try:
    exact_search = server_connection.exact_symbol_search(
        symbol="EIA%",  # Using wildcard
        user=GvWSUSERNAME,
        page=1,
        pageSize=100,
        env=example_environment
    )
    exact_search = exact_search.to_dataframe()
    print(f"Found {len(exact_search)} exact EIA symbols:")
    print(exact_search[['symbol', 'description']].head(20))
except Exception as e:
    print(f"Error with ExactSymbolSearch: {e}")

# Method 5: Use UnifiedMetadata search
print("\n5. Searching EIA symbols using UnifiedMetadata...")
try:
    unified_search = server_connection.unified_metadata(
        search="EIA",
        records_back=1000,
        page=1,
        pageSize=100,
        env=example_environment
    )
    unified_search = unified_search.to_dataframe()
    print(f"Found {len(unified_search)} EIA symbols via UnifiedMetadata:")
    print(unified_search[['symbol', 'description']].head(20))
except Exception as e:
    print(f"Error with UnifiedMetadata: {e}")

# Method 6: Get specific EIA symbol details
print("\n6. Getting specific EIA symbol details...")
try:
    specific_eia = server_connection.exact_symbol_search(
        symbol="EIAAIMPORTQTYTOTARMIFLQ",
        user=GvWSUSERNAME,
        env=example_environment
    )
    specific_eia = specific_eia.to_dataframe()
    print("Specific EIA symbol details:")
    print(specific_eia)
except Exception as e:
    print(f"Error retrieving specific EIA symbol: {e}")

# Method 7: Search by data provider (EIA is likely a data provider)
print("\n7. Searching symbols by data provider...")
try:
    # Try to find EIA as a data provider
    provider_search = server_connection.unified_metadata(
        search="EIA",
        supplier="EIA",  # EIA might be the supplier/provider
        page=1,
        pageSize=100,
        env=example_environment
    )
    provider_search = provider_search.to_dataframe()
    print(f"Found {len(provider_search)} symbols from EIA provider:")
    print(provider_search[['symbol', 'description', 'supplier']].head(20))
except Exception as e:
    print(f"Error with provider search: {e}")

# Method 8: Get all roots and filter for EIA
print("\n8. Getting all roots and filtering for EIA...")
try:
    all_roots = server_connection.get_all_roots(
        login=GvWSUSERNAME,
        search="EIA",
        env=example_environment
    )
    all_roots = all_roots.to_dataframe()
    print(f"Found {len(all_roots)} roots with EIA:")
    print(all_roots[['symbol', 'description']].head(20))
except Exception as e:
    print(f"Error getting all roots: {e}")

# Combine all EIA symbols from different searches
print("\n=== COMBINED EIA SYMBOLS RESULTS ===")
all_eia_symbols = []

# Collect from different search methods
try:
    # From permissioned roots
    eia_from_roots = mv_result[mv_result['symbol'].str.contains('EIA', na=False)]['symbol'].tolist()
    all_eia_symbols.extend(eia_from_roots)
    print(f"From permissioned roots: {len(eia_from_roots)} symbols")
except:
    pass

try:
    # From SymbolSearch
    if 'search_result' in locals() and not search_result.empty:
        eia_from_search = search_result[search_result['symbol'].str.contains('EIA', na=False)]['symbol'].tolist()
        all_eia_symbols.extend(eia_from_search)
        print(f"From SymbolSearch: {len(eia_from_search)} symbols")
except:
    pass

try:
    # From SymbolSearchMV
    if 'search_result_mv' in locals() and not search_result_mv.empty:
        eia_from_mv = search_result_mv[search_result_mv['symbol'].str.contains('EIA', na=False)]['symbol'].tolist()
        all_eia_symbols.extend(eia_from_mv)
        print(f"From SymbolSearchMV: {len(eia_from_mv)} symbols")
except:
    pass

# Remove duplicates and sort
unique_eia_symbols = sorted(list(set(all_eia_symbols)))
print(f"\nTotal unique EIA symbols found: {len(unique_eia_symbols)}")
print("Sample EIA symbols:")
for symbol in unique_eia_symbols[:20]:  # Show first 20
    print(f"  - {symbol}")

if len(unique_eia_symbols) > 20:
    print(f"  ... and {len(unique_eia_symbols) - 20} more")

# Save to CSV if needed
if unique_eia_symbols:
    import pandas as pd
    eia_df = pd.DataFrame(unique_eia_symbols, columns=['symbol'])
    eia_df.to_csv("eia_symbols.csv", index=False)
    print(f"\nSaved {len(unique_eia_symbols)} EIA symbols to eia_symbols.csv")