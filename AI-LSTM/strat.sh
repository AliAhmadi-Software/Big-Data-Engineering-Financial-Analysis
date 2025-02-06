#!/bin/bash

# خواندن لیست نمادهای سهام از فایل
while IFS= read -r symbol; do
    echo "Starting container for stock: $symbol"
    docker run -d --rm -e STOCK_SYMBOL="$symbol" lstm-stock-predictor:latest
done < symbols.txt