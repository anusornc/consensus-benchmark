import time
import statistics
from collections import defaultdict

class MetricsCollector:
    """
    Collects and calculates performance metrics during a benchmark run.

    This class records timestamps for transaction submissions, finalizations,
    block creations, and block commitments. It then uses this data to calculate
    metrics like throughput (TPS) and transaction latency. It also logs any errors
    encountered and can store custom metrics. Uses `time.perf_counter()` for
    high-precision timing.

    Attributes:
        tx_submission_timestamps (dict): tx_id -> submission_timestamp.
        tx_finalized_timestamps (dict): tx_id -> finalization_timestamp.
        tx_block_map (dict): tx_id -> block_id where tx was finalized.
        block_creation_timestamps (dict): block_id -> creation_timestamp.
        block_commit_timestamps (dict): block_id -> commit_timestamp (when block is final).
        block_tx_counts (dict): block_id -> number of transactions in the block.
        benchmark_start_time (float | None): Timestamp when the benchmark started.
        benchmark_end_time (float | None): Timestamp when the benchmark ended.
        errors (list[str]): List of error messages recorded during the benchmark.
        custom_metrics (defaultdict[str, list]): Dictionary to store custom named metrics.
    """
    def __init__(self):
        """Initializes a new MetricsCollector instance with empty data structures."""
        self.tx_submission_timestamps: dict[str, float] = {}
        self.tx_finalized_timestamps: dict[str, float] = {}
        self.tx_block_map: dict[str, any] = {}

        self.block_creation_timestamps: dict[any, float] = {}
        self.block_commit_timestamps: dict[any, float] = {}
        self.block_tx_counts: dict[any, int] = {}

        self.benchmark_start_time: float | None = None
        self.benchmark_end_time: float | None = None

        self.errors: list[str] = []
        self.custom_metrics: defaultdict[str, list] = defaultdict(list)

    def start_benchmark(self):
        """Marks the start of the benchmark period using a high-precision timer."""
        self.benchmark_start_time = time.perf_counter()
        print(f"Benchmark started at: {self.benchmark_start_time:.4f}")

    def end_benchmark(self):
        """Marks the end of the benchmark period using a high-precision timer."""
        self.benchmark_end_time = time.perf_counter()
        print(f"Benchmark ended at: {self.benchmark_end_time:.4f}")

    def record_tx_submission(self, tx_id: str, timestamp: float = None):
        """
        Records the submission time of a transaction.

        Args:
            tx_id (str): The unique identifier of the transaction.
            timestamp (float, optional): The submission timestamp.
                                         Defaults to `time.perf_counter()`.
        """
        self.tx_submission_timestamps[tx_id] = timestamp if timestamp is not None else time.perf_counter()

    def record_tx_finalized(self, tx_id: str, block_id: any, timestamp: float = None):
        """
        Records the finalization time of a transaction within a specific block.

        Args:
            tx_id (str): The unique identifier of the transaction.
            block_id (any): The identifier of the block where the transaction was finalized.
            timestamp (float, optional): The finalization timestamp.
                                         Defaults to `time.perf_counter()`.
        """
        final_time = timestamp if timestamp is not None else time.perf_counter()
        if tx_id in self.tx_submission_timestamps:
            self.tx_finalized_timestamps[tx_id] = final_time
            self.tx_block_map[tx_id] = block_id
        else:
            # Log an error if a transaction is finalized without a prior submission record.
            self.record_error(f"Tx {tx_id} finalized in block {block_id} but no submission time recorded.")
            # Optionally, still record finalization time for other metrics if needed
            # self.tx_finalized_timestamps[tx_id] = final_time
            # self.tx_block_map[tx_id] = block_id

    def record_block_created(self, block_id: any, timestamp: float = None):
        """
        Records the creation/proposal time of a block.

        Args:
            block_id (any): The unique identifier of the block.
            timestamp (float, optional): The block creation timestamp.
                                         Defaults to `time.perf_counter()`.
        """
        self.block_creation_timestamps[block_id] = timestamp if timestamp is not None else time.perf_counter()

    def record_block_committed(self, block_id: any, num_transactions: int, timestamp: float = None):
        """
        Records the time a block is considered committed or final by the consensus.

        Also records the number of transactions in that block. If the block's creation
        time wasn't explicitly recorded, it defaults to this commit time.

        Args:
            block_id (any): The unique identifier of the block.
            num_transactions (int): The number of transactions in this block.
            timestamp (float, optional): The block commit timestamp.
                                         Defaults to `time.perf_counter()`.
        """
        self.block_commit_timestamps[block_id] = timestamp if timestamp is not None else time.perf_counter()
        self.block_tx_counts[block_id] = num_transactions
        if block_id not in self.block_creation_timestamps:
            self.block_creation_timestamps[block_id] = self.block_commit_timestamps[block_id]


    def record_custom_metric(self, metric_name: str, value: any):
        """
        Records a value for a custom-named metric.
        Multiple values can be recorded for the same metric name.

        Args:
            metric_name (str): The name of the custom metric.
            value (any): The value to record for this metric.
        """
        self.custom_metrics[metric_name].append(value)

    def record_error(self, error_message: str, context_info: dict = None):
        """
        Logs an error message encountered during the benchmark.

        Args:
            error_message (str): The description of the error.
            context_info (dict, optional): Additional contextual information about the error.
        """
        full_error_msg = error_message
        if context_info:
            full_error_msg += f" | Context: {str(context_info)}"
        self.errors.append(full_error_msg)

    def calculate_latencies(self) -> list[float]:
        """
        Calculates individual transaction latencies.
        Latency is the time difference between finalization and submission.
        Only calculates for transactions with both timestamps recorded.
        Logs an error for transactions with negative latency.

        Returns:
            list[float]: A list of calculated transaction latencies in seconds.
        """
        latencies = []
        for tx_id, final_time in self.tx_finalized_timestamps.items():
            if tx_id in self.tx_submission_timestamps:
                latency = final_time - self.tx_submission_timestamps[tx_id]
                if latency >= 0:
                    latencies.append(latency)
                else:
                    self.record_error(f"Negative latency for tx {tx_id}",
                                      {"final_time": final_time,
                                       "submission_time": self.tx_submission_timestamps[tx_id]})
        return latencies

    def get_summary(self) -> dict:
        """
        Calculates and returns a summary of all collected benchmark metrics.

        The summary includes benchmark duration, transaction counts, throughput (TPS)
        calculated in two ways (based on individual transaction finalizations and
        based on transactions in committed blocks), and various latency statistics
        (average, median, min, max, standard deviation, P99).

        Returns:
            dict: A dictionary containing the benchmark summary. Returns an error message
                  if the benchmark was not properly started or ended.
        """
        if self.benchmark_start_time is None or self.benchmark_end_time is None:
            return {"error": "Benchmark not started or ended properly."}

        duration = self.benchmark_end_time - self.benchmark_start_time
        if duration <= 1e-9: # Avoid division by zero or skewed results from extremely short runs
            duration = 1e-9

        # TPS based on individually finalized transactions with latency data
        num_finalized_txs_with_latency = len([
            tx_id for tx_id in self.tx_finalized_timestamps
            if tx_id in self.tx_submission_timestamps
        ])
        tps_finalized_with_latency = num_finalized_txs_with_latency / duration

        # TPS based on transactions in blocks committed within the benchmark window
        first_block_commit_time_in_window = float('inf')
        last_block_commit_time_in_window = float('-inf')
        txs_in_window_committed_blocks = 0

        for block_id, commit_time in self.block_commit_timestamps.items():
            # Consider blocks whose commit time falls within the benchmark duration
            if self.benchmark_start_time <= commit_time <= self.benchmark_end_time:
                first_block_commit_time_in_window = min(first_block_commit_time_in_window, commit_time)
                last_block_commit_time_in_window = max(last_block_commit_time_in_window, commit_time)
                txs_in_window_committed_blocks += self.block_tx_counts.get(block_id, 0)

        block_commit_window_duration = last_block_commit_time_in_window - first_block_commit_time_in_window
        if txs_in_window_committed_blocks > 0 and block_commit_window_duration > 0:
            tps_block_based_window = txs_in_window_committed_blocks / block_commit_window_duration
        elif txs_in_window_committed_blocks > 0: # Only one block commit event or all at same instant
            tps_block_based_window = txs_in_window_committed_blocks / duration # Use overall duration
        else:
            tps_block_based_window = 0.0

        latencies = self.calculate_latencies()
        avg_latency = statistics.mean(latencies) if latencies else None
        median_latency = statistics.median(latencies) if latencies else None
        min_latency = min(latencies) if latencies else None
        max_latency = max(latencies) if latencies else None
        stdev_latency = statistics.stdev(latencies) if len(latencies) > 1 else None

        # Calculate P99 latency if enough data points exist
        p99_latency = None
        if len(latencies) >= 100: # statistics.quantiles needs at least 2 points for n=100 (99th percentile is index 98)
            p99_latency = statistics.quantiles(latencies, n=100)[98]
        elif latencies: # Fallback for fewer than 100 points
            p99_latency = max(latencies)

        num_blocks_committed_in_window = len([
            t for t in self.block_commit_timestamps.values()
            if self.benchmark_start_time <= t <= self.benchmark_end_time
        ])

        summary = {
            "benchmark_duration_seconds": round(duration, 4),
            "total_transactions_submitted": len(self.tx_submission_timestamps),
            "total_transactions_finalized_with_latency": num_finalized_txs_with_latency,
            "total_transactions_in_committed_blocks_in_window": txs_in_window_committed_blocks,
            "total_blocks_committed_in_window": num_blocks_committed_in_window,
            "tps_based_on_finalized_tx_with_latency": round(tps_finalized_with_latency, 3),
            "tps_based_on_committed_blocks_in_window": round(tps_block_based_window, 3),
            "avg_tx_latency_seconds": round(avg_latency, 6) if avg_latency is not None else "N/A",
            "median_tx_latency_seconds": round(median_latency, 6) if median_latency is not None else "N/A",
            "min_tx_latency_seconds": round(min_latency, 6) if min_latency is not None else "N/A",
            "max_tx_latency_seconds": round(max_latency, 6) if max_latency is not None else "N/A",
            "stdev_tx_latency_seconds": round(stdev_latency, 6) if stdev_latency is not None else "N/A",
            "p99_tx_latency_seconds": round(p99_latency, 6) if p99_latency is not None else "N/A",
            "num_errors": len(self.errors),
            "first_3_errors": self.errors[:3] # Show a sample of errors
        }

        # Add summaries for custom metrics
        for name, values in self.custom_metrics.items():
            if values and all(isinstance(v, (int, float)) for v in values): # Only for numeric lists
                summary[f"custom_{name}_avg"] = round(statistics.mean(values), 3)
                summary[f"custom_{name}_sum"] = round(sum(values), 3)
                summary[f"custom_{name}_count"] = len(values)
            elif values:
                 summary[f"custom_{name}_count"] = len(values) # For non-numeric custom metrics, just count
        return summary

if __name__ == '__main__':
    collector = MetricsCollector()

    print("--- Simulating Benchmark Run ---")
    collector.start_benchmark()

    # Simulate tx submissions
    for i in range(10):
        collector.record_tx_submission(f"tx{i}")
        time.sleep(0.01) # Simulate some delay between submissions

    # Simulate block commitments and tx finalizations
    time.sleep(0.1) # Simulate time to form first block
    collector.record_block_created("block1", timestamp=collector.benchmark_start_time + 0.1)
    collector.record_block_committed(block_id="block1", num_transactions=5, timestamp=collector.benchmark_start_time + 0.12)
    for i in range(5):
        collector.record_tx_finalized(f"tx{i}", block_id="block1", timestamp=collector.benchmark_start_time + 0.12)

    time.sleep(0.08) # Simulate time to form second block
    collector.record_block_created("block2", timestamp=collector.benchmark_start_time + 0.20)
    collector.record_block_committed(block_id="block2", num_transactions=3, timestamp=collector.benchmark_start_time + 0.22)
    for i in range(5, 8): # tx5, tx6, tx7
        collector.record_tx_finalized(f"tx{i}", block_id="block2", timestamp=collector.benchmark_start_time + 0.22)

    # tx8, tx9 are submitted but not finalized in this run before benchmark ends
    time.sleep(0.05) # Ensure benchmark runs for a bit longer
    collector.end_benchmark()

    summary_report = collector.get_summary()
    import json # Ensure json is imported for the __main__ block
    print("\n--- Benchmark Summary ---")
    print(json.dumps(summary_report, indent=2))

    # Example with an error
    collector_err = MetricsCollector()
    collector_err.start_benchmark()
    collector_err.record_tx_finalized("tx_orphan", "block_orphan") # Finalized without submission
    collector_err.record_error("A test error occurred", {"details": "some context"})
    collector_err.end_benchmark()
    summary_err = collector_err.get_summary()
    print("\n--- Benchmark Summary (with error example) ---")
    print(json.dumps(summary_err, indent=2))
