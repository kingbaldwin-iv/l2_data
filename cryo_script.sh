cryo logs \
    --label v3 \
    --blocks 114200000:130045411 \
    --event-signature "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)" \
    --topic0 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67 \
    --output-dir opv3 \
    --max-concurrent-requests 50 \
    --n-chunks 2000 \
    --no-report \
    --rpc disco_op
