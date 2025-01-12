import polars as pl
from web3 import Web3, HTTPProvider
import os
from typing import Union, List, Set, Dict, Optional
import logging
import pathlib
import json

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DEBUG = True


def chain_setup(
    dir_name: str,
    rpc: str,
    label: Optional[str] = "",
):
    w3 = Web3(HTTPProvider(rpc))
    data_path = f"./{dir_name}/*.parquet"
    df = pl.read_parquet(data_path)
    chain_id: int = df["chain_id"][0]
    merged_data = f"swap{label}_logs_{chain_id}.parquet"
    df.write_parquet(merged_data)
    logging.info("Merged Parquet files")
    new_dir_name = f"data_{chain_id}"
    os.makedirs(new_dir_name)
    set_of_add = set(df["address"].to_list())
    logging.info("Converted addies to a set")
    pool_addresses = list(
        map(lambda x: Web3.to_checksum_address("0x" + x.hex()), set_of_add)
    )
    logging.info("addies checksummed")
    MINIMAL_ABI0 = [
        {
            "inputs": [],
            "name": "token0",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "token1",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]
    pl_pool: List[Dict[str, str]] = []
    exception_pool: List[str] = []
    tokens: Set[str] = set()
    to_p = len(pool_addresses)
    done = 0
    for i in pool_addresses:
        try:
            contract = w3.eth.contract(address=i, abi=MINIMAL_ABI0)
            token0 = contract.functions.token0().call()
            token1 = contract.functions.token1().call()
            val = {"pool_address": i, "token0": token0, "token1": token1}
            tokens.add(token0)
            tokens.add(token1)
            done = done + 1
            log_str = f"Processed {done / to_p}:" + json.dumps(val, indent=2)
            logging.info(log_str)
            pl_pool.append(val)
            if DEBUG:
                break
        except Exception:
            logging.warning(f"Something went wrong with pool:{i}")
            exception_pool.append(i)

    dfw1 = pl.DataFrame(pl_pool)
    parq_pool_name = f"pools{label}_{chain_id}.parquet"
    dfw1.write_parquet(parq_pool_name)
    current_path = pathlib.Path().resolve()
    path1 = current_path / parq_pool_name
    path1.rename(current_path / new_dir_name / parq_pool_name)
    logging.info("Generated pools parquet file")
    if len(exception_pool) > 0:
        logging.warning("Failed to process following pools: " + str(exception_pool))
    MINIMAL_ABI = [
        {
            "inputs": [],
            "name": "symbol",
            "outputs": [{"name": "", "type": "string"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]
    pl_tokens: List[Dict[str, Union[str, int]]] = []
    exception_tokens: List[str] = []
    done = 0
    to_p = len(tokens)
    for i in tokens:
        try:
            contract = w3.eth.contract(address=i, abi=MINIMAL_ABI)
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            val = {"symbol": symbol, "decimal": decimals, "ca": i}
            done = done + 1
            log_str = f"Processed {done / to_p}:" + json.dumps(val, indent=2)
            logging.info(log_str)
            pl_tokens.append(val)
            if DEBUG:
                break
        except Exception:
            logging.warning(f"Failed the token {i}")
            exception_tokens.append(i)

    dfw2 = pl.DataFrame(pl_tokens)
    parq_token_name = f"tokens_{chain_id}.parquet"
    dfw2.write_parquet(parq_token_name)
    path2 = current_path / parq_token_name
    path2.rename(current_path / new_dir_name / parq_token_name)
    logging.info("Generated tokens parquet file")
    if len(exception_tokens) > 0:
        logging.warning("Failed to process following tokens: " + str(exception_tokens))
    path3 = current_path / merged_data
    path3.rename(current_path / new_dir_name / merged_data)


if __name__ == "__main__":
    # example for v3 optimism logs
    os.popen("sh cryo_script.sh")
    logging.info(
        "fetching logs, this might take seconds or hours dependidng on the block range"
    )
    chain_setup(
        dir_name="<OUTPUT_DIR_NAME>",  # --output-dir in cryo command
        rpc="<OPTIMISM_RPC_PROVIDER>",
        lable="v3",
    )
