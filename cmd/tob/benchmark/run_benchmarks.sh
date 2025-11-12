#!/bin/bash

# Run TOB benchmarks for different cluster sizes
# This tests NFR2 - Scalability

echo "========================================="
echo "TOB SCALABILITY BENCHMARKS"
echo "========================================="
echo ""
echo "Testing cluster sizes: 3, 5, 7, 9 nodes"
echo "Measuring: latency (P50/P95/P99), throughput, message load"
echo ""

# Create benchmarks directory
mkdir -p benchmarks/tob

# Cluster sizes to test
CLUSTER_SIZES=(3 5 7 9)

# Number of messages per test
NUM_MESSAGES=200

for SIZE in "${CLUSTER_SIZES[@]}"; do
    echo ""
    echo "========================================="
    echo "Running benchmark: ${SIZE} nodes"
    echo "========================================="

    OUTPUT_FILE="benchmarks/tob/benchmark_${SIZE}nodes.json"

    # Run the benchmark
    go run cmd/tob/benchmark/main.go \
        -cluster-size=${SIZE} \
        -messages=${NUM_MESSAGES} \
        -output="${OUTPUT_FILE}" \
        -quiet

    echo ""
    echo "✓ Completed benchmark for ${SIZE} nodes"
    echo "  Results saved to: ${OUTPUT_FILE}"

    # Small delay between tests
    sleep 2
done

echo ""
echo "========================================="
echo "ALL BENCHMARKS COMPLETE"
echo "========================================="
echo ""
echo "Results saved in: benchmarks/tob/"
echo ""
echo "To compare results across cluster sizes:"
echo "  ./cmd/tob/benchmark/compare_benchmarks.sh"

