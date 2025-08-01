import argparse
import asyncio
import json
import os
import sys

# Ensure src directory is in Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from benchmarking.runner import BenchmarkRunner

async def main():
    parser = argparse.ArgumentParser(description="Blockchain Consensus Benchmark CLI")

    parser.add_argument(
        "--consensus",
        type=str,
        default="poa",
        choices=["poa", "pbft", "pow", "hotstuff", "pos", "dpos"], # Added dpos
        help="Consensus protocol to benchmark (default: poa)"
    )
    parser.add_argument(
        "--tx_rate",
        type=int,
        default=10,
        help="Target transactions per second (0 for max speed, submit all at once)"
    )
    parser.add_argument(
        "--num_transactions",
        type=int,
        default=100,
        help="Total number of transactions to submit"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duration of the benchmark in seconds (overrides num_transactions if set)"
    )
    parser.add_argument(
        "--num_nodes",
        type=int,
        default=None, # Default will be set based on consensus type
        help="Number of nodes (e.g., 1 for PoA, >=4 for PBFT)"
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default="benchmark_results.json",
        help="File to save the benchmark summary (JSON format)"
    )
    parser.add_argument(
        "--trace_pbft_messages",
        action='store_true', # Sets to True if flag is present
        help="Enable detailed P2P message tracing for PBFT in the network simulator (default: False)"
    )
    # --- PoA Specific Configs (Example) ---
    parser.add_argument(
        "--poa_block_interval",
        type=float,
        default=0.2, # Default from BenchmarkRunner's PoAAdapter
        help="Interval in seconds for PoA authority to attempt block creation (default: 0.2s)"
    )
    # --- PoW Specific Configs ---
    parser.add_argument(
        "--pow_difficulty",
        type=int,
        default=4, # Default difficulty for PoW
        help="Proof of Work difficulty (number of leading zeros required in hash)"
    )
    # --- HotStuff Specific Configs ---
    parser.add_argument(
        "--hotstuff_beat_interval",
        type=float,
        default=0.5,
        help="Interval in seconds for the HotStuff pacemaker to trigger the leader (default: 0.5s)"
    )
    parser.add_argument(
        "--trace_hotstuff_messages",
        action='store_true',
        help="Enable detailed P2P message tracing for HotStuff (default: False)"
    )
    # --- PoS Specific Configs ---
    parser.add_argument(
        "--pos_block_interval",
        type=float,
        default=1.0,
        help="Interval in seconds for PoS validators to attempt block creation (default: 1.0s)"
    )
    # --- DPoS Specific Configs ---
    parser.add_argument(
        "--dpos_block_interval",
        type=float,
        default=0.5,
        help="Interval in seconds for DPoS delegates to produce blocks (default: 0.5s)"
    )
    # --- PBFT Specific Configs (Example) ---
    # (Could add more specific PBFT params like view change timeouts later if needed)

    # --- General Configs ---
    parser.add_argument(
        "--post_submission_wait",
        type=int,
        default=None, # Will use heuristic in runner if not set
        help="Seconds to wait after all transactions are submitted for them to be finalized (default: auto)"
    )


    args = parser.parse_args()

    # --- Build configuration dictionary for BenchmarkRunner ---
    config = {
        "consensus_type": args.consensus,
        "tx_rate": args.tx_rate,
        "num_transactions": args.num_transactions,
        "trace_pbft_messages": args.trace_pbft_messages
    }

    if args.duration:
        config["duration_seconds"] = args.duration

    # Set num_nodes based on consensus type if not specified
    if args.num_nodes is not None:
        config["num_nodes"] = args.num_nodes
    else:
        if args.consensus == "poa":
            config["num_nodes"] = 1 # Default for PoA
        elif args.consensus == "pbft":
            config["num_nodes"] = 4 # Default for PBFT (minimum for f=1)

    # PoA specific
    if args.consensus == "poa":
        config["poa_block_interval_seconds"] = args.poa_block_interval
    # PoW specific
    elif args.consensus == "pow":
        config["pow_difficulty"] = args.pow_difficulty
    # HotStuff specific
    elif args.consensus == "hotstuff":
        config["hotstuff_beat_interval_seconds"] = args.hotstuff_beat_interval
        config["trace_hotstuff_messages"] = args.trace_hotstuff_messages
        if args.num_nodes is None: # Default to 4 nodes for HotStuff if not specified
            config["num_nodes"] = 4
    elif args.consensus == "pos":
        config["pos_block_interval_seconds"] = args.pos_block_interval
        # num_nodes for PoS corresponds to the number of validators in the default set
        if args.num_nodes is not None:
             # This is a simplification; a real CLI might take a JSON file for validator stakes.
             # For now, we just use the default set in the adapter, num_nodes is illustrative.
             print(f"Note: --num_nodes for PoS is illustrative. Using default validator set in PoSAdapter.")
    elif args.consensus == "dpos":
        config["dpos_block_interval_seconds"] = args.dpos_block_interval
        # For DPoS, --num_nodes sets the number of delegates in the default set
        if args.num_nodes is not None:
            config["num_nodes"] = args.num_nodes # This will be used by the adapter to generate the delegate list

    if args.post_submission_wait is not None:
        config["post_submission_wait_seconds"] = args.post_submission_wait


    print("Benchmark Configuration:")
    print(json.dumps(config, indent=2))

    # --- Run the benchmark ---
    # The config passing to PoAAdapter via event loop is a workaround.
    # A cleaner way: pass config to BenchmarkRunner, which passes it to adapters.
    # BenchmarkRunner already takes config, so adapters should be modified to accept it.
    # The PoAAdapter's __init__ in my runner.py already expects `config`.

    runner = BenchmarkRunner(config)
    summary = await runner.run()

    # --- Save results ---
    if summary:
        # Create results directory if it doesn't exist, from output_file path
        output_dir = os.path.dirname(args.output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created results directory: {output_dir}")

        try:
            with open(args.output_file, 'w') as f:
                json.dump(summary, f, indent=2)
            print(f"\nBenchmark summary saved to {args.output_file}")
        except IOError as e:
            print(f"\nError saving benchmark summary to {args.output_file}: {e}")
            print("\nBenchmark Summary (stdout fallback):")
            print(json.dumps(summary, indent=2))
    else:
        print("\nBenchmark did not produce a summary.")

if __name__ == '__main__':
    asyncio.run(main())
