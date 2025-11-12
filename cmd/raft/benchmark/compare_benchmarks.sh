#!/bin/bash

# Compare Raft benchmark results across different cluster sizes
# Analyzes NFR1 (Performance) and NFR2 (Scalability)

if [ -z "$1" ]; then
    echo "Usage: $0 <results_directory>"
    echo "Example: $0 benchmark_results_20251110_123456"
    exit 1
fi

RESULTS_DIR="$1"

if [ ! -d "$RESULTS_DIR" ]; then
    echo "Error: Directory $RESULTS_DIR does not exist"
    exit 1
fi

echo "========================================"
echo "RAFT BENCHMARK COMPARISON"
echo "========================================"
echo ""
echo "Analyzing results from: $RESULTS_DIR"
echo ""

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Warning: jq is not installed. Install it for better JSON parsing."
    echo "On macOS: brew install jq"
    echo ""
    echo "Showing raw results:"
    for file in "$RESULTS_DIR"/*.json; do
        echo "=== $(basename $file) ==="
        cat "$file"
        echo ""
    done
    exit 0
fi

echo "========================================"
echo "NFR1 - PERFORMANCE METRICS"
echo "========================================"
echo ""

printf "%-12s | %-10s | %-10s | %-10s | %-10s | %-12s\n" \
    "Cluster" "Throughput" "P50 (ms)" "P95 (ms)" "P99 (ms)" "Mean (ms)"
printf "%-12s-+-%-10s-+-%-10s-+-%-10s-+-%-10s-+-%-12s\n" \
    "------------" "----------" "----------" "----------" "----------" "------------"

# Dynamically find all cluster sizes from JSON files
cluster_sizes=()
for file in "$RESULTS_DIR"/benchmark_*nodes.json; do
    if [ -f "$file" ]; then
        size=$(basename "$file" | sed 's/benchmark_//;s/nodes.json//')
        cluster_sizes+=("$size")
    fi
done

# Sort cluster sizes numerically
IFS=$'\n' cluster_sizes=($(sort -n <<<"${cluster_sizes[*]}"))
unset IFS

for size in "${cluster_sizes[@]}"; do
    file="$RESULTS_DIR/benchmark_${size}nodes.json"
    if [ -f "$file" ]; then
        throughput=$(jq -r '.throughput_cmd_per_sec' "$file" | xargs printf "%.2f")
        p50=$(jq -r '.command_latency.p50_ms' "$file" | xargs printf "%.3f")
        p95=$(jq -r '.command_latency.p95_ms' "$file" | xargs printf "%.3f")
        p99=$(jq -r '.command_latency.p99_ms' "$file" | xargs printf "%.3f")
        mean=$(jq -r '.command_latency.mean_ms' "$file" | xargs printf "%.3f")

        printf "%-12s | %-10s | %-10s | %-10s | %-10s | %-12s\n" \
            "${size} nodes" "$throughput" "$p50" "$p95" "$p99" "$mean"
    fi
done

echo ""
echo "========================================"
echo "NFR2 - SCALABILITY ANALYSIS"
echo "========================================"
echo ""

printf "%-12s | %-12s | %-12s | %-12s | %-15s\n" \
    "Cluster" "Elections" "AppendEntries" "Heartbeats" "Total RPCs"
printf "%-12s-+-%-12s-+-%-12s-+-%-12s-+-%-15s\n" \
    "------------" "------------" "------------" "------------" "---------------"

for size in "${cluster_sizes[@]}"; do
    file="$RESULTS_DIR/benchmark_${size}nodes.json"
    if [ -f "$file" ]; then
        elections=$(jq -r '.election_count' "$file")
        append=$(jq -r '.append_entries_count' "$file")
        heartbeats=$(jq -r '.heartbeat_count' "$file")
        total=$((append + heartbeats))

        printf "%-12s | %-12s | %-12s | %-12s | %-15s\n" \
            "${size} nodes" "$elections" "$append" "$heartbeats" "$total"
    fi
done


#echo ""
#echo "========================================"
#echo "SUMMARY & INSIGHTS"
#echo "========================================"
#echo ""
#
## Calculate scalability trends using first and last cluster sizes
#if [ ${#cluster_sizes[@]} -ge 2 ]; then
#    size_min="${cluster_sizes[0]}"
#    last_idx=$((${#cluster_sizes[@]} - 1))
#    size_max="${cluster_sizes[$last_idx]}"
#
#    file_min="$RESULTS_DIR/benchmark_${size_min}nodes.json"
#    file_max="$RESULTS_DIR/benchmark_${size_max}nodes.json"
#
#    if [ -f "$file_min" ] && [ -f "$file_max" ]; then
#        throughput_min=$(jq -r '.throughput_cmd_per_sec' "$file_min")
#        throughput_max=$(jq -r '.throughput_cmd_per_sec' "$file_max")
#        p95_min=$(jq -r '.command_latency.p95_ms' "$file_min")
#        p95_max=$(jq -r '.command_latency.p95_ms' "$file_max")
#        rpcs_min=$(jq -r '.append_entries_count + .heartbeat_count' "$file_min")
#        rpcs_max=$(jq -r '.append_entries_count + .heartbeat_count' "$file_max")
#
#        throughput_change=$(echo "scale=2; (($throughput_max - $throughput_min) / $throughput_min) * 100" | bc)
#        p95_change=$(echo "scale=2; (($p95_max - $p95_min) / $p95_min) * 100" | bc)
#        rpc_change=$(echo "scale=2; (($rpcs_max - $rpcs_min) / $rpcs_min) * 100" | bc)
#
#        echo "Scaling from ${size_min} to ${size_max} nodes:"
#        echo "  • Throughput change: ${throughput_change}%"
#        echo "  • P95 latency change: ${p95_change}%"
#        echo "  • RPC overhead change: ${rpc_change}%"
#        echo ""
#
#        echo "Observations:"
#        if (( $(echo "$p95_change < 50" | bc -l) )); then
#            echo "  ✓ Latency scales well with cluster size"
#        else
#            echo "  ⚠ Latency increases significantly with cluster size"
#        fi
#
#        if (( $(echo "$rpc_change < 200" | bc -l) )); then
#            echo "  ✓ RPC overhead scales reasonably"
#        else
#            echo "  ⚠ RPC overhead increases significantly"
#        fi
#    fi
#else
#    echo "Need at least 2 cluster sizes to compare trends"
#fi
#
#echo ""
#echo "Full JSON reports available in: $RESULTS_DIR/"
#echo ""

