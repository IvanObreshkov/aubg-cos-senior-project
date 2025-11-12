#!/bin/bash

# SWIM Benchmark Runner - WITH FAILURE INJECTION
# Runs benchmarks for cluster sizes: 3, 5, 7, 9 nodes with node failures every 20s
# As per NFR2 - Scalability requirements + Failure Detection testing

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
OUTPUT_DIR="$PROJECT_ROOT/benchmarks/swim"

echo "========================================="
echo "SWIM Benchmark Suite - WITH FAILURES"
echo "========================================="
echo "This will run benchmarks with failure injection"
echo "Nodes will be killed and restarted every 20s"
echo ""
echo "Cluster sizes: 3, 5, 7, 9"
echo "Duration: 120 seconds per test"
echo "========================================="
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Build the benchmark binary
echo "Building benchmark binary with failure injection..."
cd "$PROJECT_ROOT"
go build -o bin/swim-benchmark cmd/swim/benchmark/main.go
echo "✓ Build complete"
echo ""

# Run benchmarks for each cluster size
for size in 3 5 7 9; do
    echo "========================================="
    echo "Running benchmark: ${size}-node cluster"
    echo "Mode: WITH FAILURE INJECTION"
    echo "========================================="

    output_file="$OUTPUT_DIR/benchmark_${size}nodes_failures.json"

    ./bin/swim-benchmark \
        -cluster-size=$size \
        -duration=120 \
        -output="$output_file" \
        -quiet \
        -with-failures

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
echo "Failure injection results saved to: $OUTPUT_DIR"
echo ""
echo "Files created:"
echo "  - benchmark_3nodes_failures.json"
echo "  - benchmark_5nodes_failures.json"
echo "  - benchmark_7nodes_failures.json"
echo "  - benchmark_9nodes_failures.json"
echo ""
echo "To compare with normal operation results:"
echo "  ./cmd/swim/benchmark/compare_benchmarks.sh failures"
echo ""
echo "To compare normal operation (no failures):"
echo "  ./cmd/swim/benchmark/compare_benchmarks.sh"
echo ""

