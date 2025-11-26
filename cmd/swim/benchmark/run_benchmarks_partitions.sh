#!/bin/bash

# SWIM Benchmark Runner - NETWORK PARTITIONS
# Measures false positive rate via temporary network isolation
# As per SWIM Paper Section 4.2-4.3: Suspicion mechanism and refutation

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
OUTPUT_DIR="$PROJECT_ROOT/benchmarks/swim"

echo "========================================="
echo "SWIM Benchmark Suite - NETWORK PARTITIONS"
echo "========================================="
echo "This measures TRUE FALSE POSITIVE RATE"
echo "Per SWIM paper definition:"
echo "  - Healthy process (running) incorrectly marked Failed"
echo "  - Partitions last 10s (exceed 5s timeout)"
echo "  - Node is marked Failed, then proves alive"
echo "Also tracks refutation rate (suspicion safety)"
echo ""
echo "Cluster sizes: 3, 5, 7, 9"
echo "Duration: 120 seconds per test"
echo "========================================="
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Build the benchmark binary
echo "Building benchmark binary..."
cd "$PROJECT_ROOT"
go build -o bin/swim-benchmark cmd/swim/benchmark/main.go
echo "✓ Build complete"
echo ""

# Run benchmarks for each cluster size
for size in 3 5 7 9; do
    echo "========================================="
    echo "Running benchmark: ${size}-node cluster"
    echo "Mode: NETWORK PARTITIONS (False Positives)"
    echo "========================================="

    output_file="$OUTPUT_DIR/benchmark_${size}nodes_partitions.json"

    ./bin/swim-benchmark \
        -cluster-size=$size \
        -duration=120 \
        -output="$output_file" \
        -quiet \
        -with-failures \
        -failure-type=partitions

    echo ""
    echo "✓ Completed ${size}-node benchmark"
    echo "✓ Results saved to: $output_file"
    echo ""

    # Wait a bit between tests
    sleep 2
done

echo "========================================="
echo "All Benchmarks Complete!"
echo "========================================="
echo "Network partition results saved to: $OUTPUT_DIR"
echo ""
echo "Files created:"
echo "  - benchmark_3nodes_partitions.json"
echo "  - benchmark_5nodes_partitions.json"
echo "  - benchmark_7nodes_partitions.json"
echo "  - benchmark_9nodes_partitions.json"
echo ""
echo "To compare results:"
echo "  ./cmd/swim/benchmark/compare_benchmarks.sh partitions"
echo ""

