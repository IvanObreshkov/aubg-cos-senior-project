#!/bin/bash

# SWIM Benchmark Runner - CRASH-STOP FAILURES
# Measures failure detection with permanent node crashes
# As per SWIM Paper Section 4: Failure detector protocol

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
OUTPUT_DIR="$PROJECT_ROOT/benchmarks/swim"

echo "========================================="
echo "SWIM Benchmark Suite - CRASH-STOP FAILURES"
echo "========================================="
echo "This measures FAILURE DETECTION"
echo "Nodes will permanently crash (cannot refute)"
echo "Tests detection latency and accuracy"
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
    echo "Mode: CRASH-STOP FAILURES (Real crashes)"
    echo "========================================="

    output_file="$OUTPUT_DIR/benchmark_${size}nodes_crashstop.json"

    ./bin/swim-benchmark \
        -cluster-size=$size \
        -duration=120 \
        -output="$output_file" \
        -quiet \
        -with-failures \
        -failure-type=crashstop

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
echo "Crash-stop failure results saved to: $OUTPUT_DIR"
echo ""
echo "Files created:"
echo "  - benchmark_3nodes_crashstop.json"
echo "  - benchmark_5nodes_crashstop.json"
echo "  - benchmark_7nodes_crashstop.json"
echo "  - benchmark_9nodes_crashstop.json"
echo ""
echo "To compare results:"
echo "  ./cmd/swim/benchmark/compare_benchmarks.sh crashstop"
echo ""

