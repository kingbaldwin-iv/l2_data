import objcts
import json
import polars as pl
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
end: int = <END_BLOCK_OF_THE_LOG_RANGE>
start: int = <START_BLOCK_OF_THE_LOG_RANGE>
delta: int = <BLOCKS_PER_FILE>
index = 1
df2 = pl.read_parquet("<PATH_TO_SWAP_V2_LOGS>")
df3 = pl.read_parquet("<PATH_TO_SWAP_V3_LOGS>")
index = 1
range_high = 0
while range_high <= end:
    range_low = start + (index * delta)
    range_high = start + (index + 1) * delta
    index = index + 1
    df2x = df2.filter(
        (pl.col("block_number") >= range_low) & (pl.col("block_number") < range_high)
    )
    df3x = df3.filter(
        (pl.col("block_number") >= range_low) & (pl.col("block_number") < range_high)
    )
    logging.info(f"Operating on range {range_low}_{range_high}")
    list1 = objcts.SwapV3.process_log_v3(df3x)
    list2 = objcts.SwapV2.process_log_v2(df2x)
    list1.extend(list2)
    txs = objcts.Transaction.bundle_swaps(list1)
    ret = []
    for x in txs:
        a = x.analyze()
        if a[0]:
            ret.append(a[1])
            # print(json.dumps(a[1], indent=2))

    ret_df = pl.DataFrame(ret)
    print(ret_df)
    ret_df.write_parquet(f"mev_{range_low}_{range_high}.parquet")
