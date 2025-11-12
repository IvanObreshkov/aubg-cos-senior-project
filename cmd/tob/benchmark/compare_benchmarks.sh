#!/bin/bash

# Compare TOB benchmark results across different cluster sizes
# Analyzes NFR1 (Performance) and NFR2 (Scalability)

echo "========================================="
echo "TOB BENCHMARK COMPARISON"
echo "========================================="
echo ""

BENCHMARK_DIR="benchmarks/tob"

if [ ! -d "$BENCHMARK_DIR" ]; then
    echo "Error: Benchmark directory not found: $BENCHMARK_DIR"
    echo "Please run benchmarks first:"
    echo "  ./cmd/tob/benchmark/run_benchmarks.sh"
    exit 1
fi

echo "Comparing results across cluster sizes..."
echo ""

# Print header
printf "%-15s | %-12s | %-12s | %-12s | %-15s | %-15s | %-15s\n" \
    "Cluster Size" "P50 (ms)" "P95 (ms)" "P99 (ms)" "Throughput" "Msgs/Node" "Seq Latency"
echo "----------------+-------------+-------------+-------------+----------------+----------------+----------------"

# Process each benchmark file
for SIZE in 3 5 7 9; do
    FILE="${BENCHMARK_DIR}/benchmark_${SIZE}nodes.json"

    if [ ! -f "$FILE" ]; then
        printf "%-15s | %-12s | %-12s | %-12s | %-15s | %-15s | %-15s\n" \
            "${SIZE} nodes" "N/A" "N/A" "N/A" "N/A" "N/A" "N/A"
        continue
    fi

    # Extract metrics using jq
    P50=$(jq -r '.broadcast_latency.p50_ms' "$FILE" 2>/dev/null || echo "N/A")
    P95=$(jq -r '.broadcast_latency.p95_ms' "$FILE" 2>/dev/null || echo "N/A")
    P99=$(jq -r '.broadcast_latency.p99_ms' "$FILE" 2>/dev/null || echo "N/A")
    THROUGHPUT=$(jq -r '.message_throughput_msg_per_sec' "$FILE" 2>/dev/null || echo "N/A")
    MSGS_PER_NODE=$(jq -r '.messages_per_node' "$FILE" 2>/dev/null || echo "N/A")
    SEQ_LATENCY=$(jq -r '.sequencing_latency.mean_ms' "$FILE" 2>/dev/null || echo "N/A")

    # Format numbers
    if [ "$P50" != "N/A" ] && [ "$P50" != "null" ]; then
        P50=$(printf "%.3f" "$P50")
    else
        P50="N/A"
    fi

    if [ "$P95" != "N/A" ] && [ "$P95" != "null" ]; then
        P95=$(printf "%.3f" "$P95")
    else
        P95="N/A"
    fi

    if [ "$P99" != "N/A" ] && [ "$P99" != "null" ]; then
        P99=$(printf "%.3f" "$P99")
    else
        P99="N/A"
    fi

    if [ "$THROUGHPUT" != "N/A" ] && [ "$THROUGHPUT" != "null" ]; then
        THROUGHPUT=$(printf "%.2f msg/s" "$THROUGHPUT")
    else
        THROUGHPUT="N/A"
    fi

    if [ "$MSGS_PER_NODE" != "N/A" ] && [ "$MSGS_PER_NODE" != "null" ]; then
        MSGS_PER_NODE=$(printf "%.1f" "$MSGS_PER_NODE")
    else
        MSGS_PER_NODE="N/A"
    fi

    if [ "$SEQ_LATENCY" != "N/A" ] && [ "$SEQ_LATENCY" != "null" ]; then
        SEQ_LATENCY=$(printf "%.3f ms" "$SEQ_LATENCY")
    else
        SEQ_LATENCY="N/A"
    fi

    printf "%-15s | %-12s | %-12s | %-12s | %-15s | %-15s | %-15s\n" \
        "${SIZE} nodes" "$P50" "$P95" "$P99" "$THROUGHPUT" "$MSGS_PER_NODE" "$SEQ_LATENCY"
done

echo ""
echo "========================================="
echo "NFR ANALYSIS"
echo "========================================="
echo ""
echo "NFR1 - Performance (Latency):"
echo "  - P50: Median latency for message delivery"
echo "  - P95: 95th percentile latency"
echo "  - P99: 99th percentile latency"
echo "  Lower values = better performance"
echo ""
echo "NFR2 - Scalability (Throughput & Load):"
echo "  - Throughput: Messages delivered per second"
echo "  - Msgs/Node: Average message load per node"
echo "  - Seq Latency: Time for sequencer to process messages"
echo "  Watch for degradation as cluster size increases"
echo ""
echo "Expected Behavior:"
echo "  - Latency should remain relatively stable"
echo "  - Message load should increase with cluster size"
echo "  - Throughput depends on sequencer bottleneck"
echo ""

echo "========================================="

