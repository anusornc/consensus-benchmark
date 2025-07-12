import time
import asyncio # For more advanced transaction generation/rate limiting
import json # For printing summary
from uuid import uuid4

# Core components
from src.blockchain.blockchain import Blockchain
from src.transactions.transaction import Transaction

# Consensus mechanisms and helpers
from src.consensus.poa import PoAConsensus
from src.consensus.pbft import PBFTNode
from src.consensus.pow import PoWConsensus # Added PoWConsensus
from src.networking.simulator import NetworkSimulator
from .metrics import MetricsCollector


class BenchmarkRunner:
    """
    Orchestrates benchmark runs for different consensus protocols.

    This class handles setting up the blockchain environment, initializing the
    chosen consensus mechanism, generating and submitting transactions, and
    collecting performance metrics via a MetricsCollector.

    Attributes:
        config (dict): Configuration dictionary for the benchmark run.
                       Expected keys include "consensus_type", "num_nodes",
                       "tx_rate", "num_transactions", "duration_seconds", etc.
        metrics_collector (MetricsCollector): Instance for collecting benchmark data.
        blockchain (Blockchain | None): The primary blockchain instance used or referenced.
                                         For distributed protocols, this might be one node's chain
                                         or None if metrics are aggregated differently.
        consensus_adapter (object | None): An adapter specific to the consensus protocol being tested.
                                           This adapter handles protocol-specific setup and interaction.
        nodes (dict): For distributed protocols like PBFT, a dictionary of node_id to node_instance.
        network_simulator (NetworkSimulator | None): For distributed protocols, the network simulator instance.
    """
    def __init__(self, config: dict):
        """
        Initializes the BenchmarkRunner.

        Args:
            config (dict): A dictionary containing benchmark configuration parameters.
                           See `run_benchmark.py` for typical parameters.
        """
        self.config = config
        self.metrics_collector = MetricsCollector()

        self.blockchain: Blockchain | None = None
        self.consensus_adapter: object | None = None

        self.nodes: dict[str, PBFTNode] = {}
        self.network_simulator: NetworkSimulator | None = None


    def _setup_blockchain_and_consensus(self):
        """
        Initializes the blockchain, network (if applicable), and the selected
        consensus mechanism based on the benchmark configuration.
        It creates a specific "consensus adapter" to handle interactions with the
        chosen protocol.

        Raises:
            ValueError: If an unsupported consensus type is specified in the config.
        """
        consensus_type = self.config.get("consensus_type", "poa").lower()
        num_nodes = self.config.get("num_nodes", 1)

        print(f"\nSetting up benchmark for consensus: {consensus_type.upper()} with {num_nodes} node(s).")

        if consensus_type == "poa":
            self.blockchain = Blockchain(genesis_sealer_id="poa_benchmark_genesis_sealer")
            # For PoA, num_nodes typically refers to the number of authorities.
            authorities = [f"authority{i}" for i in range(max(1, num_nodes))] # Ensure at least one authority

            self.consensus_adapter = PoAAdapter(
                blockchain=self.blockchain,
                authorities=authorities,
                metrics_collector=self.metrics_collector,
                config=self.config # Pass full config
            )
            print(f"PoA setup with authorities: {authorities}")

        elif consensus_type == "pbft":
            if num_nodes < 4:
                print(f"Warning: PBFT typically requires at least 4 nodes for f=1. Running with {num_nodes}.")

            self.network_simulator = NetworkSimulator(trace_messages=self.config.get("trace_pbft_messages", False))
            node_ids = [f"N{i}" for i in range(num_nodes)]

            # Each PBFT node gets its own blockchain instance.
            node_blockchains = {nid: Blockchain(genesis_sealer_id=f"pbft_genesis_{nid}") for nid in node_ids}

            for nid in node_ids:
                node = PBFTNode(node_id=nid, all_node_ids=node_ids, blockchain=node_blockchains[nid])
                self.network_simulator.register_node(node)
                self.nodes[nid] = node

            self.blockchain = self.nodes[node_ids[0]].blockchain if node_ids else None # Handle case of 0 nodes
            self.consensus_adapter = PBFTAdapter(
                nodes=self.nodes,
                network_simulator=self.network_simulator,
                metrics_collector=self.metrics_collector,
                config=self.config # Pass full config
            )
            if self.nodes and node_ids:
                 print(f"PBFT setup with nodes: {node_ids}. Primary for View 0: {self.nodes[node_ids[0]].primary_id}")
            else:
                 print("PBFT setup with no nodes (num_nodes likely < 1). This will not work as expected.")

        elif consensus_type == "pow":
            self.blockchain = Blockchain(genesis_sealer_id="pow_benchmark_genesis_sealer")
            # PoW difficulty can be set in config, e.g., config.get("pow_difficulty", 4)
            difficulty = self.config.get("pow_difficulty", 4)
            self.consensus_adapter = PoWAdapter(
                blockchain=self.blockchain,
                difficulty=difficulty,
                metrics_collector=self.metrics_collector,
                config=self.config
            )
            print(f"PoW setup with difficulty: {difficulty}")

        else:
            raise ValueError(f"Unsupported consensus type: {consensus_type}")

    async def _generate_and_submit_transactions(self):
        """
        Asynchronously generates and submits transactions to the consensus mechanism
        via the configured adapter, at a specified rate.
        Records submission time for each transaction.
        """
        tx_rate = self.config.get("tx_rate", 10)  # Transactions per second
        num_transactions = self.config.get("num_transactions", 100)

        sleep_interval = 1.0 / tx_rate if tx_rate > 0 else 0

        print(f"Submitting {num_transactions} transactions at a target rate of ~{tx_rate if tx_rate > 0 else 'max'} TPS.")

        for i in range(num_transactions):
            asset_val = f"asset_bm_{uuid4().hex[:8]}"
            creator_val = f"agent_bm_{i % self.config.get('num_unique_submitters', 10)}"

            tx = Transaction(
                transaction_type="asset_creation",
                asset_id_value=asset_val,
                agent_ids={"creator_agent_id": creator_val},
                additional_props={"benchmark_tx_seq": i, "load_gen_instance": "runner_1"}
            )

            self.metrics_collector.record_tx_submission(tx.transaction_id)

            submission_successful = await self.consensus_adapter.submit_transaction(tx)
            if not submission_successful:
                self.metrics_collector.record_error(f"Failed to submit transaction {tx.transaction_id}")

            if sleep_interval > 0:
                await asyncio.sleep(sleep_interval)
            elif i % 1000 == 0 and i > 0 :
                await asyncio.sleep(0)

        print(f"Finished submitting {num_transactions} transactions.")
        if hasattr(self.consensus_adapter, 'no_more_transactions'):
            await self.consensus_adapter.no_more_transactions()


    async def run(self) -> dict:
        """
        Executes the benchmark run.

        This involves:
        1. Setting up the blockchain and consensus mechanism.
        2. Starting the metrics collector.
        3. Asynchronously generating and submitting transactions.
        4. Waiting for the benchmark to complete (either by duration or by processing
           all submitted transactions, with a timeout).
        5. Stopping metrics collection and generating a summary report.

        Returns:
            dict: A summary of benchmark metrics from the MetricsCollector.
        """
        self._setup_blockchain_and_consensus()
        self.metrics_collector.start_benchmark()

        submit_task = asyncio.create_task(self._generate_and_submit_transactions())

        duration_seconds = self.config.get("duration_seconds")
        num_target_transactions = self.config.get("num_transactions")

        if duration_seconds:
            print(f"Benchmark will run for {duration_seconds} seconds.")
            await asyncio.sleep(duration_seconds)
            if hasattr(self.consensus_adapter, 'stop_processing'):
                await self.consensus_adapter.stop_processing()
        else:
            await submit_task
            print("All transactions submitted. Waiting for finalization or consensus adapter signal...")
            if hasattr(self.consensus_adapter, 'wait_for_completion'):
                 await self.consensus_adapter.wait_for_completion(num_target_transactions)
            else:
                default_wait = self.config.get("post_submission_wait_seconds", 10)
                print(f"Consensus adapter has no explicit wait_for_completion. Waiting {default_wait}s.")
                await asyncio.sleep(default_wait)


        self.metrics_collector.end_benchmark()
        summary = self.metrics_collector.get_summary()

        print("\n--- Benchmark Run Summary ---")
        print(json.dumps(summary, indent=2)) # json import is now at the top
        return summary


# --- Adapters for different consensus mechanisms ---
class PoAAdapter:
    """
    Adapter for the Proof of Authority (PoA) consensus mechanism.
    Manages interaction with PoAConsensus for block creation and transaction processing
    during a benchmark run.
    """
    def __init__(self, blockchain: Blockchain, authorities: list[str],
                 metrics_collector: MetricsCollector, config: dict):
        """
        Initializes the PoAAdapter.

        Args:
            blockchain (Blockchain): The blockchain instance.
            authorities (list[str]): List of authority identifiers.
            metrics_collector (MetricsCollector): Collector for benchmark metrics.
            config (dict): Benchmark configuration, used for PoA specific settings like block interval.
        """
        self.blockchain = blockchain
        self.poa_consensus = PoAConsensus(authorities)
        self.metrics_collector = metrics_collector
        self.config = config
        self.authorities = authorities
        self.current_authority_idx = 0
        self._processing_task: asyncio.Task | None = None
        self._stop_processing_event = asyncio.Event()
        self._all_tx_submitted_event = asyncio.Event()

    async def submit_transaction(self, tx: Transaction) -> bool:
        """
        Submits a transaction to the PoA system by adding it to the blockchain's
        pending transaction pool.

        Args:
            tx (Transaction): The transaction to submit.

        Returns:
            bool: True if submission was successful (transaction added to pool).
        """
        self.blockchain.add_transaction(tx)
        return True

    async def no_more_transactions(self):
        """Signals that all transactions for the benchmark have been submitted."""
        self._all_tx_submitted_event.set()

    async def stop_processing(self):
        """Signals the block processing loop to stop."""
        print("PoAAdapter: Received signal to stop processing blocks.")
        self._stop_processing_event.set()

    async def _process_blocks(self):
        """
        Internal task that simulates an authority creating blocks.
        It periodically checks for pending transactions and attempts to create a new block
        if transactions are available. Stops when signaled or all transactions are processed.
        """
        print("PoAAdapter: Starting block processing loop...")
        block_interval = self.config.get("poa_block_interval_seconds", 0.1)

        while not self._stop_processing_event.is_set():
            if self.blockchain.pending_transactions:
                authority = self.authorities[self.current_authority_idx % len(self.authorities)]
                tx_list_for_block = list(self.blockchain.pending_transactions)

                new_block = self.poa_consensus.create_block(
                    blockchain_instance=self.blockchain,
                    authority_address=authority,
                    transactions=tx_list_for_block
                )
                if new_block:
                    if self.blockchain.add_block(new_block):
                        self.metrics_collector.record_block_committed(
                            new_block.index, len(new_block.transactions), time.perf_counter()
                        )
                        for tx_dict in new_block.transactions:
                            self.metrics_collector.record_tx_finalized(
                                tx_dict['transaction_id'], new_block.index, time.perf_counter()
                            )
                        self.current_authority_idx += 1
                    else:
                        err_msg = f"PoAAdapter: Blockchain rejected block {new_block.index} from {authority}."
                        print(err_msg)
                        self.metrics_collector.record_error(err_msg)
                else:
                    self.current_authority_idx += 1

            elif self._all_tx_submitted_event.is_set() and not self.blockchain.pending_transactions:
                print("PoAAdapter: All submitted transactions processed and pending queue is empty.")
                self._stop_processing_event.set()
                break

            try:
                await asyncio.wait_for(asyncio.sleep(block_interval), timeout=block_interval + 0.05)
            except asyncio.TimeoutError:
                pass
            if self._stop_processing_event.is_set(): break

        print("PoAAdapter: Block processing loop stopped.")


    async def wait_for_completion(self, num_target_transactions: int):
        """
        Waits for the PoA simulation to process the target number of transactions
        or until a timeout occurs. Starts the block processing task if not already running.

        Args:
            num_target_transactions (int): The number of transactions expected to be finalized.
        """
        if self._processing_task is None:
            self._processing_task = asyncio.create_task(self._process_blocks())

        start_wait = time.perf_counter()
        timeout_base = self.config.get("post_submission_wait_seconds", 10)
        timeout_per_tx = self.config.get("timeout_per_tx_factor", 0.5)
        timeout_seconds = timeout_base + (num_target_transactions * timeout_per_tx)

        while not self._stop_processing_event.is_set():
            finalized_count = len(self.metrics_collector.tx_finalized_timestamps)
            if finalized_count >= num_target_transactions:
                print(f"PoAAdapter: Target of {num_target_transactions} transactions finalized.")
                self._stop_processing_event.set()
                break
            if time.perf_counter() - start_wait > timeout_seconds:
                print(f"PoAAdapter: Timeout waiting for completion. {finalized_count}/{num_target_transactions} finalized.")
                self._stop_processing_event.set()
                break

            await asyncio.sleep(0.1)

        if self._processing_task and not self._processing_task.done():
            try:
                await asyncio.wait_for(self._processing_task, timeout=5.0)
            except asyncio.TimeoutError:
                print("PoAAdapter: Timeout waiting for processing task to finish after stop signal.")
                self._processing_task.cancel()


class PBFTAdapter:
    """
    Adapter for the Practical Byzantine Fault Tolerance (PBFT) consensus mechanism.
    Manages interaction with PBFTNodes and the NetworkSimulator for benchmarking.
    It patches `execute_request` on PBFT nodes to hook into metrics collection.
    """
    def __init__(self, nodes: dict, network_simulator: NetworkSimulator,
                 metrics_collector: MetricsCollector, config: dict):
        """
        Initializes the PBFTAdapter.

        Args:
            nodes (dict): A dictionary of node_id to PBFTNode instances.
            network_simulator (NetworkSimulator): The network simulator instance.
            metrics_collector (MetricsCollector): Collector for benchmark metrics.
            config (dict): Benchmark configuration, used for PBFT specific settings.
        """
        self.nodes = nodes
        self.network_simulator = network_simulator
        self.metrics_collector = metrics_collector
        self.config = config
        self.primary_node_id: str | None = None
        self._find_initial_primary()
        self._all_tx_submitted_event = asyncio.Event()
        # self._processed_tx_count = 0 # This will be implicitly tracked by MetricsCollector now
        self.globally_committed_seq_nums = set() # Tracks (view, seq_num) to prevent overcounting metrics

        self._patch_pbft_nodes_for_metrics()

    def _find_initial_primary(self):
        """Identifies the initial primary node based on view 0."""
        if not self.nodes: return
        for node_id, node in self.nodes.items():
            if node.is_primary():
                self.primary_node_id = node_id
                print(f"PBFTAdapter: Initial primary identified as {self.primary_node_id} for view {node.current_view}.")
                return
        self.primary_node_id = list(self.nodes.keys())[0]
        print(f"PBFTAdapter: Fallback initial primary: {self.primary_node_id}.")

    def _patch_pbft_nodes_for_metrics(self):
        """
        Monkey-patches the `execute_request` method of all registered PBFT nodes
        to allow the adapter to record metrics upon successful block execution.
        """
        for node_id, node_obj in self.nodes.items():
            original_execute_request = node_obj.execute_request

            def metrics_execute_request_wrapper(view, seq_num, _original_method=original_execute_request, _node_instance=node_obj, _adapter_self=self):
                _original_method(view, seq_num)

                log_entry = _node_instance.message_log[view][seq_num]
                if log_entry['status'] == 'executed':
                    executed_block = _node_instance.blockchain.last_block
                    if executed_block and executed_block.nonce == seq_num:
                        request_key = (view, seq_num) # Use (view, seq_num) as a unique key

                        if request_key not in _adapter_self.globally_committed_seq_nums:
                            _adapter_self.globally_committed_seq_nums.add(request_key)

                            global_block_id = f"pbft_v{view}_s{seq_num}" # Global ID for this committed request
                            _adapter_self.metrics_collector.record_block_committed(
                                block_id=global_block_id,
                                num_transactions=len(executed_block.transactions),
                                timestamp=time.perf_counter()
                            )
                            for tx_dict in executed_block.transactions:
                                _adapter_self.metrics_collector.record_tx_finalized(
                                    tx_id=tx_dict['transaction_id'],
                                    block_id=global_block_id,
                                    timestamp=time.perf_counter()
                                )
                        # If request_key is already in globally_committed_seq_nums, this node's execution
                        # of the same agreed request is noted, but metrics aren't re-recorded globally.
                    else:
                        _adapter_self.metrics_collector.record_error(
                            f"PBFTAdapter: Mismatch mapping executed seq_num {seq_num} to block on node {_node_instance.node_id}. Last block nonce: {executed_block.nonce if executed_block else 'None'}")

            node_obj.execute_request = metrics_execute_request_wrapper


    async def submit_transaction(self, tx: Transaction) -> bool:
        """
        Submits a transaction to the current primary PBFT node.

        Args:
            tx (Transaction): The transaction to submit.

        Returns:
            bool: True if submission was attempted (actual success depends on PBFT processing).
        """
        current_primary_id = ""
        if self.nodes: # Ensure nodes dict is not empty
             # Get current primary from any node (they should all agree on current view's primary)
            any_node_id = list(self.nodes.keys())[0]
            current_primary_id = self.nodes[any_node_id].primary_id

        if self.primary_node_id != current_primary_id and current_primary_id:
            print(f"PBFTAdapter: Primary changed from {self.primary_node_id} to {current_primary_id}")
            self.primary_node_id = current_primary_id

        if not self.primary_node_id:
            self.metrics_collector.record_error("PBFTAdapter: No primary PBFT node identified for submission.")
            return False

        primary_node = self.nodes.get(self.primary_node_id)
        if not primary_node:
            self.metrics_collector.record_error(f"PBFTAdapter: Primary node {self.primary_node_id} instance not found for submission.")
            return False

        primary_node.handle_client_request(tx.to_dict())
        return True

    async def no_more_transactions(self):
        """Signals that all transactions for the benchmark have been submitted."""
        self._all_tx_submitted_event.set()

    async def stop_processing(self):
        pass

    async def wait_for_completion(self, num_target_transactions: int):
        """
        Waits for the PBFT simulation to process the target number of transactions
        or until a timeout occurs. Completion is tracked by counting finalized transactions
        via the patched `execute_request` method.

        Args:
            num_target_transactions (int): The number of transactions expected to be finalized.
        """
        start_wait = time.perf_counter()
        timeout_base = self.config.get("post_submission_wait_seconds", 20)
        timeout_per_tx = self.config.get("timeout_per_tx_factor_pbft", 1.0)
        timeout_seconds = timeout_base + (num_target_transactions * timeout_per_tx)

        print(f"PBFTAdapter: Waiting for {num_target_transactions} transactions to be finalized. Timeout: {timeout_seconds:.2f}s")

        while True:
            finalized_tx_ids_count = len(self.metrics_collector.tx_finalized_timestamps.keys())

            if finalized_tx_ids_count >= num_target_transactions:
                print(f"PBFTAdapter: Target of {num_target_transactions} transactions finalized ({finalized_tx_ids_count} unique IDs recorded).")
                break
            if time.perf_counter() - start_wait > timeout_seconds:
                print(f"PBFTAdapter: Timeout waiting for completion. {finalized_tx_ids_count}/{num_target_transactions} finalized.")
                break

            await asyncio.sleep(0.5)


class PoWAdapter:
    """
    Adapter for the Proof of Work (PoW) consensus mechanism.
    Simulates a single miner finding proofs and creating blocks.
    """
    def __init__(self, blockchain: Blockchain, difficulty: int,
                 metrics_collector: MetricsCollector, config: dict):
        """
        Initializes the PoWAdapter.

        Args:
            blockchain (Blockchain): The blockchain instance.
            difficulty (int): The PoW difficulty (e.g., number of leading zeros).
            metrics_collector (MetricsCollector): Collector for benchmark metrics.
            config (dict): Benchmark configuration.
        """
        self.blockchain = blockchain
        self.pow_consensus = PoWConsensus(difficulty)
        self.metrics_collector = metrics_collector
        self.config = config
        self._processing_task: asyncio.Task | None = None
        self._stop_processing_event = asyncio.Event()
        self._all_tx_submitted_event = asyncio.Event()
        self.miner_id = self.config.get("pow_miner_id", "benchmark_pow_miner")

    async def submit_transaction(self, tx: Transaction) -> bool:
        """Adds transaction to the blockchain's pending pool."""
        self.blockchain.add_transaction(tx)
        # In PoW, block creation is not immediate upon tx submission by default.
        # Start mining if not already, or rely on periodic mining.
        if self._processing_task is None or self._processing_task.done():
            self._ensure_mining_is_running()
        return True

    async def no_more_transactions(self):
        """Signals that all transactions have been submitted."""
        self._all_tx_submitted_event.set()
        # Continue mining any remaining transactions.

    async def stop_processing(self):
        """Signals the mining loop to stop."""
        print("PoWAdapter: Received signal to stop mining.")
        self._stop_processing_event.set()

    def _ensure_mining_is_running(self):
        """Starts the mining task if it's not already running."""
        if self._processing_task is None or self._processing_task.done():
            print("PoWAdapter: Starting mining process...")
            self._processing_task = asyncio.create_task(self._mine_blocks())


    async def _mine_blocks(self):
        """
        Internal task that simulates a miner creating blocks via Proof of Work.
        Continuously tries to mine a new block if there are pending transactions
        or if configured to mine empty blocks (not currently supported).
        """
        while not self._stop_processing_event.is_set():
            if not self.blockchain.pending_transactions:
                if self._all_tx_submitted_event.is_set():
                    print("PoWAdapter: All submitted transactions processed, no more pending. Stopping mining.")
                    self._stop_processing_event.set()
                    break
                # No transactions, wait a bit before checking again or stop if configured.
                # This simple version will keep checking.
                try:
                    await asyncio.wait_for(asyncio.sleep(0.1), timeout=0.15)
                except asyncio.TimeoutError:
                    pass
                if self._stop_processing_event.is_set(): break
                continue

            tx_list_for_block = list(self.blockchain.pending_transactions)

            # Perform Proof of Work
            # The proof_of_work method is blocking, so for truly async behavior,
            # it would need to be run in an executor if it's CPU-bound and long.
            # For simulation, direct call is fine but will block this task.
            print(f"PoWAdapter: Starting PoW for block {self.blockchain.last_block.index + 1} with {len(tx_list_for_block)} txs...")

            # Explicitly import Block here for testing task scope
            from src.blockchain.block import Block

            mined_info = self.pow_consensus.proof_of_work(
                self.blockchain.last_block,
                tx_list_for_block,
                miner_id=self.miner_id
            )

            if self._stop_processing_event.is_set(): break # Check immediately after potentially long PoW

            if mined_info:
                nonce, new_hash, timestamp = mined_info
                new_block = Block(
                    index=self.blockchain.last_block.index + 1,
                    transactions=tx_list_for_block, # These are dicts
                    timestamp=timestamp,
                    previous_hash=self.blockchain.last_block.hash,
                    nonce=nonce,
                    sealer_id=self.miner_id
                )
                # The block's hash is calculated on init. It must match new_hash.
                if new_block.hash != new_hash:
                    self.metrics_collector.record_error(f"PoWAdapter: Mined hash {new_hash} != block hash {new_block.hash}")
                    continue # Skip this block

                if self.pow_consensus.validate_proof(new_block):
                    if self.blockchain.add_block(new_block):
                        print(f"PoWAdapter: Mined and added Block {new_block.index} with {len(new_block.transactions)} txs. Nonce: {nonce}.")
                        self.metrics_collector.record_block_committed(
                            new_block.index, len(new_block.transactions), time.perf_counter()
                        )
                        for tx_dict in new_block.transactions:
                            self.metrics_collector.record_tx_finalized(
                                tx_dict['transaction_id'], new_block.index, time.perf_counter()
                            )
                    else:
                        self.metrics_collector.record_error(f"PoWAdapter: Blockchain rejected mined block {new_block.index}")
                else:
                    self.metrics_collector.record_error(f"PoWAdapter: Mined block {new_block.index} failed PoW validation.")
            else:
                # proof_of_work might return None if interrupted, though not implemented yet
                print("PoWAdapter: Mining did not produce a result (possibly interrupted).")

            # Yield control briefly to allow other tasks to run, especially if mining was quick
            await asyncio.sleep(0)

        print("PoWAdapter: Mining loop stopped.")

    async def wait_for_completion(self, num_target_transactions: int):
        """
        Waits for the PoW simulation to process the target number of transactions
        or until a timeout occurs. Ensures the mining task is running.
        """
        self._ensure_mining_is_running() # Start mining task if not already

        start_wait = time.perf_counter()
        timeout_base = self.config.get("post_submission_wait_seconds", 20)
        # PoW can be slow, timeout needs to be generous or based on expected mining time per tx
        timeout_per_tx = self.config.get("timeout_per_tx_factor_pow", 2.0) # Allow more time per tx for PoW
        timeout_seconds = timeout_base + (num_target_transactions * timeout_per_tx)

        print(f"PoWAdapter: Waiting for {num_target_transactions} transactions to be finalized. Timeout: {timeout_seconds:.2f}s")

        while not self._stop_processing_event.is_set():
            finalized_count = len(self.metrics_collector.tx_finalized_timestamps.keys())
            if finalized_count >= num_target_transactions:
                print(f"PoWAdapter: Target of {num_target_transactions} transactions finalized.")
                self._stop_processing_event.set()
                break
            if time.perf_counter() - start_wait > timeout_seconds:
                print(f"PoWAdapter: Timeout waiting for completion. {finalized_count}/{num_target_transactions} finalized.")
                self._stop_processing_event.set()
                break
            await asyncio.sleep(0.2)

        if self._processing_task and not self._processing_task.done():
            try:
                await asyncio.wait_for(self._processing_task, timeout=5.0)
            except asyncio.TimeoutError:
                print("PoWAdapter: Timeout waiting for mining task to finish after stop signal.")
                self._processing_task.cancel()

# Removed __main__ block to isolate syntax error cause.
# The main execution logic is now in the top-level run_benchmark.py script.
