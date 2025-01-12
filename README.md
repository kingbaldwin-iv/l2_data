### Missing fields:
- main.py: <OUTPUT_DIR_NAME>, <OPTIMISM_RPC_PROVIDER>
- cryo_script.sh: <LABEL>, <START_BLOCK>, <END_BLOCK>, <OUTPUT_DIR_NAME>, <MESC_ALIAS>
### Additional requirements:
- [Mesc](https://github.com/paradigmxyz/mesc)
- [Cryo](https://github.com/paradigmxyz/cryo)
### Resulting Strcuture
>
    .
    ├── ...
    ├── data_{chain_id}                    
    │   ├── swap{label}_logs_{chain_id}.parquet          
    │   ├── pools{label}_{chain_id}.parquet         
    │   └── tokens_{chain_id}.parquet                
    └── ...
### Data Schema
- Swap file: depends on the event signature e.g. for v3 swaps:
  | Field Name | Type |
  |------------|------|
  | block_number | `u32` |
  | transaction_index | `u32` |
  | log_index | `u32` |
  | transaction_hash | `binary` |
  | address | `binary` |
  | topic0 | `binary` |
  | n_data_bytes | `u32` |
  | chain_id | `u64` |
  | event__sender | `binary` |
  | event__recipient | `binary` |
  | event__amount0_binary | `binary` |
  | event__amount0_string | `str` |
  | event__amount0_f64 | `f64` |
  | event__amount1_binary | `binary` |
  | event__amount1_string | `str` |
  | event__amount1_f64 | `f64` |
  | event__sqrtPriceX96_binary | `binary` |
  | event__sqrtPriceX96_string | `str` |
  | event__sqrtPriceX96_f64 | `f64` |
  | event__liquidity_binary | `binary` |
  | event__liquidity_string | `str` |
  | event__liquidity_f64 | `f64` |
  | event__tick | `i64` |
- Token file:
  | Field Name | Type |
  |------------|------|
  | symbol | `str` |
  | decimal | `i64` |
  | ca | `str` |
- Pool file:
  | Field Name | Type |
  |------------|------|
  | pool_address | `str` |
  | token0 | `str` |
  | token1 | `str` |
