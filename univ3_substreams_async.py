import asyncio
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

from subgrounds import AsyncSubgrounds

# Get command line date parameters
start_date = sys.argv[1]
date_range = int(sys.argv[2])

# Convert the start date from string to datetime object
start_date = datetime.strptime(start_date, "%Y-%m-%d")

# Create date ranges based on the provided start date and number of days
date_ranges = [
    (start_date, start_date + timedelta(days=1))
    for start_date in [start_date + timedelta(days=i) for i in range(0, date_range, 1)]
]

protocol_name = "uniswap_v3"
# Create a data folder if it doesn't exist
if not os.path.exists(f"data/{protocol_name}"):
    os.makedirs(f"data/{protocol_name}")


async def run_query(date_range: tuple[datetime, datetime]) -> pd.DataFrame:
    async with AsyncSubgrounds() as sg:
        # https://thegraph.com/explorer/subgraphs/HUZDsRpEVP2AvzDCyzDHtdc64dyDxx8FQjzsmqSg4H3B?view=Overview&chain=arbitrum-one
        deployment_id: str = "QmQJovmQLigEwkMWGjMT8GbeS2gjDytqWCGL58BEhLu9Ag"

        subgraph = await sg.load_subgraph(
            f"https://api.playgrounds.network/v1/proxy/deployments/id/{deployment_id}"
        )

        t0 = time.perf_counter()
        start_date, end_date = date_range
        try:
            filename = f"data/{protocol_name}/swaps_{start_date.date()}.csv"
            if not os.path.exists(filename):
                swaps_qp = subgraph.Query.swaps(
                    first=250000,
                    where=[
                        subgraph.Swap.timestamp >= int(start_date.timestamp()),
                        subgraph.Swap.timestamp <= int(end_date.timestamp()),
                    ],
                )
                print(f"Query for {start_date.date()} started")
                # Await the result of the asynchronous call
                df: pd.DataFrame = await sg.query_df([
                    swaps_qp.timestamp,
                    swaps_qp.transaction.id,
                    swaps_qp.transaction.blockNumber,
                    swaps_qp.transaction.timestamp,
                    swaps_qp.sqrtPriceX96,
                    swaps_qp.pool.id,
                    swaps_qp.pool.liquidity,
                    swaps_qp.pool.token0Price,
                    swaps_qp.pool.token1Price,
                    swaps_qp.recipient,
                    swaps_qp.sender,
                    swaps_qp.origin,
                    swaps_qp.amount0,
                    swaps_qp.amount1,
                    swaps_qp.amountUSD,
                    swaps_qp.token0.name,
                    swaps_qp.token0.decimals,
                    swaps_qp.token0.symbol,
                    swaps_qp.token1.name,
                    swaps_qp.token1.decimals,
                    swaps_qp.token1.symbol,
                ])
                print(df.shape)
                t1 = time.perf_counter()
                print(
                    f"Query for {start_date.date()} completed in {t1 - t0:0.2f}s")

                # insert custom data features
                df.insert(0, "protocol", protocol_name)

                # Save the DataFrame
                df.to_csv(filename)

                return df

        except Exception as e:
            print(f"Error querying for {start_date.date()}: {e}")
            # Optionally, you can log the error or take other actions as needed.
            return None


async def run_query_with_semaphore(semaphore, date_range: tuple[datetime, datetime]) -> pd.DataFrame:
    async with semaphore:
        return await run_query(date_range)


async def main():
    t0 = time.perf_counter()

    # Limit the number of concurrent async calls to 15
    semaphore = asyncio.Semaphore(15)

    # Use functools.partial to pass the semaphore to the run_query_with_semaphore function
    tasks = [run_query_with_semaphore(semaphore, date_range)
             for date_range in date_ranges]
    await asyncio.gather(*tasks)

    t1 = time.perf_counter()
    print(f"Async Queries completed in {t1 - t0:0.2f}s ")


if __name__ == "__main__":
    asyncio.run(main())
