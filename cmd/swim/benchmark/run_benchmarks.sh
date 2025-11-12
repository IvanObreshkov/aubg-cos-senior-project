#!/bin/bash

# SWIM Benchmark Runner
# Runs benchmarks for cluster sizes: 3, 5, 7, 9 nodes
# As per NFR2 - Scalability requirements

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
OUTPUT_DIR="$PROJECT_ROOT/benchmarks/swim"

echo "========================================="
echo "SWIM Benchmark Suite"
echo "========================================="
echo "This will run benchmarks for cluster sizes: 3, 5, 7, 9"
echo "Each test runs for 60 seconds"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Build the benchmark binary
echo "Building benchmark binary..."
cd "$PROJECT_ROOT"
go build -o bin/swim-benchmark cmd/swim/benchmark/main.go
echo "✓ Build complete"
echo ""
echo "Note: Running WITHOUT failure injection (steady-state only)"
echo ""

# Run benchmarks for each cluster size
for size in 3 5 7 9; do
    echo "========================================="
    echo "Running benchmark: ${size}-node cluster"
    echo "========================================="

    output_file="$OUTPUT_DIR/benchmark_${size}nodes.json"

    ./bin/swim-benchmark \
        -cluster-size=$size \
        -duration=60 \
        -output="$output_file" \
        -quiet

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
echo "Results saved to: $OUTPUT_DIR"
echo ""
echo "To compare results, run:"
echo "  ./cmd/swim/benchmark/compare_benchmarks.sh"
echo ""

