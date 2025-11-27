#!/bin/bash

# SWIM Benchmark Comparison Script
# Compares results across different cluster sizes

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
OUTPUT_DIR="$PROJECT_ROOT/benchmarks/swim"

# Check if comparing failure results
MODE="${1:-normal}"
if [ "$MODE" == "partitions" ]; then
    FILE_SUFFIX="_partitions"
    MODE_NAME="NETWORK PARTITIONS (False Positives)"
elif [ "$MODE" == "crashstop" ]; then
    FILE_SUFFIX="_crashstop"
    MODE_NAME="CRASH-STOP FAILURES (Real Crashes)"
elif [ "$MODE" == "failures" ]; then
    FILE_SUFFIX="_failures"
    MODE_NAME="WITH FAILURE INJECTION (Legacy)"
else
    FILE_SUFFIX=""
    MODE_NAME="NORMAL OPERATION"
fi

echo "========================================="
echo "SWIM Benchmark Comparison"
echo "Mode: $MODE_NAME"
echo "========================================="
echo ""

if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Error: No benchmark results found at $OUTPUT_DIR"
    if [ "$MODE" == "partitions" ]; then
        echo "Please run ./cmd/swim/benchmark/run_benchmarks_partitions.sh first"
    elif [ "$MODE" == "crashstop" ]; then
        echo "Please run ./cmd/swim/benchmark/run_benchmarks_crashstop.sh first"
    elif [ "$MODE" == "failures" ]; then
        echo "Please run ./cmd/swim/benchmark/run_benchmarks_failures.sh first"
    else
        echo "Please run ./cmd/swim/benchmark/run_benchmarks.sh first"
    fi
    exit 1
fi

# Check if jq is available for JSON parsing
if ! command -v jq &> /dev/null; then
    echo "Warning: jq not found. Install jq for better JSON processing."
    echo "Showing raw JSON files instead."
    echo ""

    for size in 3 5 7 9; do
        file="$OUTPUT_DIR/benchmark_${size}nodes${FILE_SUFFIX}.json"
        if [ -f "$file" ]; then
            echo "=== ${size}-Node Cluster ==="
            cat "$file"
            echo ""
        fi
    done
    exit 0
fi

echo "NFR1 - Performance Metrics (Latency)"
echo "-------------------------------------"
printf "%-10s | %-12s | %-12s | %-12s\n" "Cluster" "P50 (ms)" "P95 (ms)" "Mean (ms)"
printf "%-10s-+-%-12s-+-%-12s-+-%-12s\n" "----------" "------------" "------------" "------------"

for size in 3 5 7 9; do
    file="$OUTPUT_DIR/benchmark_${size}nodes${FILE_SUFFIX}.json"
    if [ -f "$file" ]; then
        p50=$(jq -r '.probe_latency.p50_ms // "N/A"' "$file")
        p95=$(jq -r '.probe_latency.p95_ms // "N/A"' "$file")
        mean=$(jq -r '.probe_latency.mean_ms // "N/A"' "$file")
        # Format to 3 decimal places if numeric
        if [ "$p50" != "N/A" ]; then p50=$(printf "%.3f" "$p50"); fi
        if [ "$p95" != "N/A" ]; then p95=$(printf "%.3f" "$p95"); fi
        if [ "$mean" != "N/A" ]; then mean=$(printf "%.3f" "$mean"); fi
        printf "%-10s | %-12s | %-12s | %-12s\n" "${size} nodes" "$p50" "$p95" "$mean"
    fi
done

echo ""
echo "NFR2 - Scalability Metrics (Message Load)"
echo "-------------------------------------"
printf "%-10s | %-15s | %-15s | %-18s\n" "Cluster" "Total Messages" "Throughput" "Msg/Node"
printf "%-10s-+-%-15s-+-%-15s-+-%-18s\n" "----------" "---------------" "---------------" "------------------"

for size in 3 5 7 9; do
    file="$OUTPUT_DIR/benchmark_${size}nodes${FILE_SUFFIX}.json"
    if [ -f "$file" ]; then
        total_in=$(jq -r '.total_messages_in // 0' "$file")
        total_out=$(jq -r '.total_messages_out // 0' "$file")
        total=$((total_in + total_out))
        throughput=$(jq -r '.message_throughput_msg_per_sec // "N/A"' "$file")
        msg_per_node=$(jq -r '.messages_per_node // "N/A"' "$file")
        # Format to 3 decimal places if numeric
        if [ "$throughput" != "N/A" ]; then throughput=$(printf "%.3f" "$throughput"); fi
        if [ "$msg_per_node" != "N/A" ]; then msg_per_node=$(printf "%.3f" "$msg_per_node"); fi
        printf "%-10s | %-15s | %-15s | %-18s\n" "${size} nodes" "$total" "$throughput" "$msg_per_node"
    fi
done

echo ""
echo "NFR3 - Reliability Metrics"
echo "-------------------------------------"
echo ""
echo "Safety Metrics (Suspicion Mechanism Effectiveness)"
printf "%-10s | %-15s | %-15s | %-15s\n" "Cluster" "Suspicions" "Refuted" "Refutation %"
printf "%-10s-+-%-15s-+-%-15s-+-%-15s\n" "----------" "---------------" "---------------" "---------------"

for size in 3 5 7 9; do
    file="$OUTPUT_DIR/benchmark_${size}nodes${FILE_SUFFIX}.json"
    if [ -f "$file" ]; then
        suspicions=$(jq -r '.suspicion_count // 0' "$file")
        refuted=$(jq -r '.refuted_suspicion_count // 0' "$file")
        refutation_rate=$(jq -r '.refutation_rate * 100 // 0' "$file")
        printf "%-10s | %-15s | %-15s | %-15.2f\n" "${size} nodes" "$suspicions" "$refuted" "$refutation_rate"
    fi
done

echo ""
echo "Accuracy Metrics (False Positives per SWIM Paper)"
printf "%-10s | %-15s | %-15s | %-15s\n" "Cluster" "Failures" "False Positive" "FP Rate %"
printf "%-10s-+-%-15s-+-%-15s-+-%-15s\n" "----------" "---------------" "---------------" "---------------"

for size in 3 5 7 9; do
    file="$OUTPUT_DIR/benchmark_${size}nodes${FILE_SUFFIX}.json"
    if [ -f "$file" ]; then
        detections=$(jq -r '.failure_detection_count // 0' "$file")
        false_positives=$(jq -r '.false_positive_count // 0' "$file")
        fp_rate=$(jq -r '.false_positive_rate * 100 // 0' "$file")
        printf "%-10s | %-15s | %-15s | %-15.2f\n" "${size} nodes" "$detections" "$false_positives" "$fp_rate"
    fi
done

# Show failure detection latency if in failure mode
if [ "$MODE" == "failures" ]; then
    echo ""
    echo "Failure Detection Latency"
    echo "-------------------------------------"
    printf "%-10s | %-12s | %-12s | %-12s\n" "Cluster" "P50 (ms)" "P95 (ms)" "Mean (ms)"
    printf "%-10s-+-%-12s-+-%-12s-+-%-12s\n" "----------" "------------" "------------" "------------"

    for size in 3 5 7 9; do
        file="$OUTPUT_DIR/benchmark_${size}nodes${FILE_SUFFIX}.json"
        if [ -f "$file" ]; then
            p50=$(jq -r '.failure_detection_latency.p50_ms // "N/A"' "$file")
            p95=$(jq -r '.failure_detection_latency.p95_ms // "N/A"' "$file")
            mean=$(jq -r '.failure_detection_latency.mean_ms // "N/A"' "$file")
            # Format to 3 decimal places if numeric and not zero
            if [ "$p50" != "N/A" ] && [ "$p50" != "0" ]; then p50=$(printf "%.3f" "$p50"); fi
            if [ "$p95" != "N/A" ] && [ "$p95" != "0" ]; then p95=$(printf "%.3f" "$p95"); fi
            if [ "$mean" != "N/A" ] && [ "$mean" != "0" ]; then mean=$(printf "%.3f" "$mean"); fi
            printf "%-10s | %-12s | %-12s | %-12s\n" "${size} nodes" "$p50" "$p95" "$mean"
        fi
    done

    echo ""
    echo "Member Join Latency (Recovery)"
    echo "-------------------------------------"
    printf "%-10s | %-12s | %-12s | %-12s\n" "Cluster" "P50 (ms)" "P95 (ms)" "Mean (ms)"
    printf "%-10s-+-%-12s-+-%-12s-+-%-12s\n" "----------" "------------" "------------" "------------"

    for size in 3 5 7 9; do
        file="$OUTPUT_DIR/benchmark_${size}nodes${FILE_SUFFIX}.json"
        if [ -f "$file" ]; then
            p50=$(jq -r '.member_join_latency.p50_ms // "N/A"' "$file")
            p95=$(jq -r '.member_join_latency.p95_ms // "N/A"' "$file")
            mean=$(jq -r '.member_join_latency.mean_ms // "N/A"' "$file")
            # Format to 3 decimal places if numeric and not zero
            if [ "$p50" != "N/A" ] && [ "$p50" != "0" ]; then p50=$(printf "%.3f" "$p50"); fi
            if [ "$p95" != "N/A" ] && [ "$p95" != "0" ]; then p95=$(printf "%.3f" "$p95"); fi
            if [ "$mean" != "N/A" ] && [ "$mean" != "0" ]; then mean=$(printf "%.3f" "$mean"); fi
            printf "%-10s | %-12s | %-12s | %-12s\n" "${size} nodes" "$p50" "$p95" "$mean"
        fi
    done
fi

echo ""
echo "Message Type Breakdown (9-node cluster)"
echo "-------------------------------------"
file="$OUTPUT_DIR/benchmark_9nodes${FILE_SUFFIX}.json"
if [ -f "$file" ]; then
    ping=$(jq -r '.ping_count // 0' "$file")
    ack=$(jq -r '.ack_count // 0' "$file")
    ping_req=$(jq -r '.ping_req_count // 0' "$file")
    indirect=$(jq -r '.indirect_ping_count // 0' "$file")

    echo "  Ping:         $ping"
    echo "  Ack:          $ack"
    echo "  PingReq:      $ping_req"
    echo "  IndirectPing: $indirect"
fi

echo ""
echo "========================================="
echo "Analyzed files:"
for size in 3 5 7 9; do
    file="$OUTPUT_DIR/benchmark_${size}nodes${FILE_SUFFIX}.json"
    if [ -f "$file" ]; then
        echo "  ✓ benchmark_${size}nodes${FILE_SUFFIX}.json"
    else
        echo "  ✗ benchmark_${size}nodes${FILE_SUFFIX}.json (not found)"
    fi
done
echo ""
echo "To compare different modes:"
if [ "$MODE" == "failures" ]; then
    echo "  Normal operation:  ./cmd/swim/benchmark/compare_benchmarks.sh"
    echo "  Failures (current): ./cmd/swim/benchmark/compare_benchmarks.sh failures"
elif [ "$MODE" == "partitions" ]; then
    echo "  Normal operation:  ./cmd/swim/benchmark/compare_benchmarks.sh"
    echo "  Partitions (current): ./cmd/swim/benchmark/compare_benchmarks.sh partitions"
elif [ "$MODE" == "crashstop" ]; then
    echo "  Normal operation:  ./cmd/swim/benchmark/compare_benchmarks.sh"
    echo "  Crash-Stop (current): ./cmd/swim/benchmark/compare_benchmarks.sh crashstop"
else
    echo "  Normal operation (current): ./cmd/swim/benchmark/compare_benchmarks.sh"
    echo "  Failures:          ./cmd/swim/benchmark/compare_benchmarks.sh failures"
    echo "  Partitions:        ./cmd/swim/benchmark/compare_benchmarks.sh partitions"
    echo "  Crash-Stop:        ./cmd/swim/benchmark/compare_benchmarks.sh crashstop"
fi
echo "========================================="
