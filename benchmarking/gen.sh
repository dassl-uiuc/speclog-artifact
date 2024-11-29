#!/bin/bash

get_cdf() {
    # 检查是否提供了文件路径和搜索字符串
    if [ $# -ne 2 ]; then
        echo "Usage: $0 <file_path> <search_string>"
        exit 1
    fi

    file_path="$1"
    search_string="$2"

    # 查找字符串的最后一次出现的行号
    line_number=$(grep -n "$search_string" "$file_path" | tail -n 1 | cut -d: -f1)

    # 如果找到了字符串，打印之后的100行
    if [ -n "$line_number" ]; then
        start_line=$((line_number + 1))
        tail -n +$start_line "$file_path" | head -n 100
    else
        echo "String not found in the file."
    fi
}

mkdir -p ./batch_int
for ((i=0; i<6; i++))
do
    for ((j=0; j<2; j++))
    do
        get_cdf "./results/logs/0.8ms/append_bench_80/data-$i-$j.log" "local cut interreport histogram" > ./batch_int/data-$i-$j.cdf
    done
done
