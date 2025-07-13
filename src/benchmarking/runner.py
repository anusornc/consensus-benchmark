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
from src.consensus.pow import PoWConsensus
from src.consensus.pos import PoSConsensus
from src.consensus.dpos import DPoSConsensus # Added DPoS
from src.consensus.hotstuff import HotStuffNode
from src.networking.simulator import NetworkSimulator
from .metrics import MetricsCollector


class BenchmarkRunner:
    """
    Orchestrates benchmark runs for different consensus protocols.
    """
    def __init__(self, config: dict):
        """
        Initializes the BenchmarkRunner.
        """
        self.config = config
        self.metrics_collector = MetricsCollector()

        self.blockchain: Blockchain | None = None
        self.consensus_adapter: object | None = None

        self.nodes: dict[str, any] = {}
        self.network_simulator: NetworkSimulator | None = None


    def _setup_blockchain_and_consensus(self):
        """
        Initializes the blockchain, network (if applicable), and the selected
        consensus mechanism based on the benchmark configuration.
        """
        consensus_type = self.config.get("consensus_type", "poa").lower()
        num_nodes = self.config.get("num_nodes", 1)

        print(f"\nSetting up benchmark for consensus: {consensus_type.upper()} with {num_nodes} node(s).")

        if consensus_type == "poa":
            self.blockchain = Blockchain(genesis_sealer_id="poa_benchmark_genesis_sealer")
            authorities = [f"authority{i}" for i in range(max(1, num_nodes))]
            self.consensus_adapter = PoAAdapter(
                blockchain=self.blockchain,
                authorities=authorities,
                metrics_collector=self.metrics_collector,
                config=self.config
            )
            print(f"PoA setup with authorities: {authorities}")

        elif consensus_type == "pbft":
            if num_nodes < 4:
                print(f"Warning: PBFT typically requires at least 4 nodes for f=1. Running with {num_nodes}.")
            self.network_simulator = NetworkSimulator(trace_messages=self.config.get("trace_pbft_messages", False))
            node_ids = [f"N{i}" for i in range(num_nodes)]
            node_blockchains = {nid: Blockchain(genesis_sealer_id=f"pbft_genesis_{nid}") for nid in node_ids}
            for nid in node_ids:
                node = PBFTNode(node_id=nid, all_node_ids=node_ids, blockchain=node_blockchains[nid])
                self.network_simulator.register_node(node)
                self.nodes[nid] = node
            self.blockchain = self.nodes[node_ids[0]].blockchain if node_ids else None
            self.consensus_adapter = PBFTAdapter(
                nodes=self.nodes,
                network_simulator=self.network_simulator,
                metrics_collector=self.metrics_collector,
                config=self.config
            )
            if self.nodes and node_ids:
                 print(f"PBFT setup with nodes: {node_ids}. Primary for View 0: {self.nodes[node_ids[0]].primary_id}")

        elif consensus_type == "pow":
            self.blockchain = Blockchain(genesis_sealer_id="pow_benchmark_genesis_sealer")
            difficulty = self.config.get("pow_difficulty", 4)
            self.consensus_adapter = PoWAdapter(
                blockchain=self.blockchain,
                difficulty=difficulty,
                metrics_collector=self.metrics_collector,
                config=self.config
            )
            print(f"PoW setup with difficulty: {difficulty}")

        elif consensus_type == "hotstuff":
            if num_nodes < 4:
                print(f"Warning: HotStuff (BFT) typically requires at least 4 nodes. Using {num_nodes}.")
            self.network_simulator = NetworkSimulator(trace_messages=self.config.get("trace_hotstuff_messages", False))
            node_ids = [f"HS_N{i}" for i in range(num_nodes)]
            node_blockchains = {nid: Blockchain(genesis_sealer_id=f"hotstuff_genesis_{nid}") for nid in node_ids}
            for nid in node_ids:
                node = HotStuffNode(node_id=nid, all_node_ids=node_ids, blockchain=node_blockchains[nid])
                self.network_simulator.register_node(node)
                self.nodes[nid] = node
            self.blockchain = self.nodes[node_ids[0]].blockchain if node_ids else None
            self.consensus_adapter = HotStuffAdapter(
                nodes=self.nodes,
                network_simulator=self.network_simulator,
                metrics_collector=self.metrics_collector,
                config=self.config
            )
            if self.nodes and node_ids:
                print(f"HotStuff setup with nodes: {node_ids}. Primary for View 0: {self.nodes[node_ids[0]].primary_id}")

        elif consensus_type == "pos":
            self.blockchain = Blockchain(genesis_sealer_id="pos_benchmark_genesis_sealer")
            default_validators = {"validator-A": 50, "validator-B": 30, "validator-C": 20}
            validators = self.config.get("pos_validators", default_validators)
            self.consensus_adapter = PoSAdapter(
                blockchain=self.blockchain,
                validators=validators,
                metrics_collector=self.metrics_collector,
                config=self.config
            )
            print(f"PoS setup with {len(validators)} validators.")

        elif consensus_type == "dpos":
            self.blockchain = Blockchain(genesis_sealer_id="dpos_benchmark_genesis_sealer")
            default_delegates = [f"delegate-{i}" for i in range(self.config.get("num_nodes", 21))]
            delegates = self.config.get("dpos_delegates", default_delegates)
            self.consensus_adapter = DPoSAdapter(
                blockchain=self.blockchain,
                delegates=delegates,
                metrics_collector=self.metrics_collector,
                config=self.config
            )
            print(f"DPoS setup with {len(delegates)} delegates.")

        else:
            raise ValueError(f"Unsupported consensus type: {consensus_type}")

    async def _generate_and_submit_transactions(self):
        """
        Asynchronously generates and submits transactions to the consensus mechanism.
        """
        tx_rate = self.config.get("tx_rate", 10)
        num_transactions = self.config.get("num_transactions", 100)
        sleep_interval = 1.0 / tx_rate if tx_rate > 0 else 0

        print(f"Submitting {num_transactions} transactions at a target rate of ~{tx_rate if tx_rate > 0 else 'max'} TPS.")

        for i in range(num_transactions):
            asset_val = f"asset_bm_{uuid4().hex[:8]}"
            creator_val = f"agent_bm_{i % self.config.get('num_unique_submitters', 10)}"
            tx = Transaction(
                transaction_type="asset_creation", asset_id_value=asset_val,
                agent_ids={"creator_agent_id": creator_val},
                additional_props={"benchmark_tx_seq": i}
            )
            self.metrics_collector.record_tx_submission(tx.transaction_id)
            submission_successful = await self.consensus_adapter.submit_transaction(tx)
            if not submission_successful:
                self.metrics_collector.record_error(f"Failed to submit transaction {tx.transaction_id}")
            if sleep_interval > 0:
                await asyncio.sleep(sleep_interval)
            elif i % 1000 == 0 and i > 0:
                await asyncio.sleep(0)

        print(f"Finished submitting {num_transactions} transactions.")
        if hasattr(self.consensus_adapter, 'no_more_transactions'):
            await self.consensus_adapter.no_more_transactions()


    async def run(self) -> dict:
        """
        Executes the benchmark run.
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
        print(json.dumps(summary, indent=2))
        return summary


# --- Adapters for different consensus mechanisms ---

class PoAAdapter:
    """Adapter for the Proof of Authority (PoA) consensus mechanism."""
    def __init__(self, blockchain: Blockchain, authorities: list[str],
                 metrics_collector: MetricsCollector, config: dict):
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
        self.blockchain.add_transaction(tx)
        if self._processing_task is None or self._processing_task.done():
            self._processing_task = asyncio.create_task(self._process_blocks())
        return True

    async def no_more_transactions(self):
        self._all_tx_submitted_event.set()

    async def stop_processing(self):
        self._stop_processing_event.set()

    async def _process_blocks(self):
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
                        self.metrics_collector.record_error(f"PoAAdapter: Blockchain rejected block {new_block.index} from {authority}.")
                else:
                    self.current_authority_idx += 1
            elif self._all_tx_submitted_event.is_set() and not self.blockchain.pending_transactions:
                print("PoAAdapter: All submitted transactions processed.")
                self._stop_processing_event.set()
                break
            try:
                await asyncio.wait_for(asyncio.sleep(block_interval), timeout=block_interval + 0.05)
            except asyncio.TimeoutError: pass
            if self._stop_processing_event.is_set(): break
        print("PoAAdapter: Block processing loop stopped.")

    async def wait_for_completion(self, num_target_transactions: int):
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
                self._processing_task.cancel()

class PBFTAdapter:
    """Adapter for the Practical Byzantine Fault Tolerance (PBFT) consensus mechanism."""
    def __init__(self, nodes: dict, network_simulator: NetworkSimulator,
                 metrics_collector: MetricsCollector, config: dict):
        self.nodes = nodes
        self.network_simulator = network_simulator
        self.metrics_collector = metrics_collector
        self.config = config
        self.primary_node_id: str | None = None
        self._find_initial_primary()
        self._all_tx_submitted_event = asyncio.Event()
        self.globally_committed_seq_nums = set()

        self._patch_pbft_nodes_for_metrics()

    def _find_initial_primary(self):
        """Find the initial primary node based on PBFT view 0 calculation."""
        if not self.nodes:
            return

        # Primary for view 0 is node at index (0 % n)
        node_ids = sorted(list(self.nodes.keys()))

        # Verify which node actually considers itself primary
        for node_id, node in self.nodes.items():
            if node.is_primary():
                self.primary_node_id = node_id
                print(f"PBFTAdapter: Found primary node {node_id} for view {node.current_view}")
                return

        # Fallback to calculated primary
        self.primary_node_id = node_ids[0]
        print(f"PBFTAdapter: Using fallback primary {self.primary_node_id}")

    def _patch_pbft_nodes_for_metrics(self):
        for node_id, node_obj in self.nodes.items():
            original_execute_request = node_obj.execute_request
            def metrics_execute_request_wrapper(view, seq_num, _original_method=original_execute_request, _node_instance=node_obj, _adapter_self=self):
                _original_method(view, seq_num)
                log_entry = _node_instance.message_log[view][seq_num]
                if log_entry['status'] == 'executed':
                    executed_block = _node_instance.blockchain.last_block
                    if executed_block and executed_block.nonce == seq_num:
                        request_key = (view, seq_num)
                        if request_key not in _adapter_self.globally_committed_seq_nums:
                            _adapter_self.globally_committed_seq_nums.add(request_key)
                            global_block_id = f"pbft_v{view}_s{seq_num}"
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
                    else:
                        _adapter_self.metrics_collector.record_error(f"PBFTAdapter: Mismatch mapping seq_num {seq_num} to block on node {_node_instance.node_id}.")
            node_obj.execute_request = metrics_execute_request_wrapper

    def _ensure_processing_is_running(self):
        """Ensure PBFT processing loop is running."""
        if self._processing_task is None or self._processing_task.done():
            print("PBFTAdapter: Starting PBFT processing loop...")
            self._processing_task = asyncio.create_task(self._pbft_processing_loop())

    async def _pbft_processing_loop(self):
        """Processing loop to handle PBFT operations."""
        while not self._stop_processing_event.is_set():
            # Check if primary has pending requests and should start pre-prepare
            if self.primary_node_id and self.primary_node_id in self.nodes:
                primary_node = self.nodes[self.primary_node_id]
                if primary_node.pending_client_requests and primary_node.is_primary():
                    # Trigger pre-prepare if not already in progress
                    if not any(log_entry['status'] == 'pre-preparing'
                              for view_log in primary_node.message_log.values()
                              for log_entry in view_log.values()):
                        primary_node.initiate_pre_prepare()

            await asyncio.sleep(0.1)  # Check every 100ms

    async def submit_transaction(self, tx: Transaction) -> bool:
        if not self.primary_node_id:
            self.metrics_collector.record_error("PBFTAdapter: No primary PBFT node identified.")
            return False

        primary_node = self.nodes.get(self.primary_node_id)
        if not primary_node:
            self.metrics_collector.record_error(f"PBFTAdapter: Primary node {self.primary_node_id} not found.")
            return False

        primary_node.handle_client_request(tx.to_dict())

        # Ensure processing is running
        self._ensure_processing_is_running()

        return True

    async def no_more_transactions(self):
        self._all_tx_submitted_event.set()

    async def stop_processing(self): pass

    async def wait_for_completion(self, num_target_transactions: int):
        start_wait = time.perf_counter()
        timeout_base = self.config.get("post_submission_wait_seconds", 20)
        timeout_per_tx = self.config.get("timeout_per_tx_factor_pbft", 1.0)
        timeout_seconds = timeout_base + (num_target_transactions * timeout_per_tx)
        print(f"PBFTAdapter: Waiting for {num_target_transactions} txs to finalize. Timeout: {timeout_seconds:.2f}s")
        while True:
            finalized_tx_ids_count = len(self.metrics_collector.tx_finalized_timestamps.keys())
            if finalized_tx_ids_count >= num_target_transactions:
                print(f"PBFTAdapter: Target of {num_target_transactions} finalized.")
                break
            if time.perf_counter() - start_wait > timeout_seconds:
                print(f"PBFTAdapter: Timeout waiting for completion. {finalized_tx_ids_count}/{num_target_transactions} finalized.")
                break
            await asyncio.sleep(0.5)

class PoWAdapter:
    """Adapter for the Proof of Work (PoW) consensus mechanism."""
    def __init__(self, blockchain: Blockchain, difficulty: int,
                 metrics_collector: MetricsCollector, config: dict):
        self.blockchain = blockchain
        self.pow_consensus = PoWConsensus(difficulty)
        self.metrics_collector = metrics_collector
        self.config = config
        self._processing_task: asyncio.Task | None = None
        self._stop_processing_event = asyncio.Event()
        self._all_tx_submitted_event = asyncio.Event()
        self.miner_id = self.config.get("pow_miner_id", "benchmark_pow_miner")
        self._mining_lock = asyncio.Lock()

    async def submit_transaction(self, tx: Transaction) -> bool:
        self.blockchain.add_transaction(tx)
        async with self._mining_lock:
            if self._processing_task is None or self._processing_task.done():
                self._ensure_mining_is_running()
        return True

    async def no_more_transactions(self):
        self._all_tx_submitted_event.set()

    async def stop_processing(self):
        self._stop_processing_event.set()

    def _ensure_mining_is_running(self):
        if self._processing_task is None or self._processing_task.done():
            print("PoWAdapter: Starting mining process...")
            self._processing_task = asyncio.create_task(self._mine_blocks())

    async def _mine_blocks(self):
        from src.blockchain.block import Block # Local import for async task scope
        while not self._stop_processing_event.is_set():
            if not self.blockchain.pending_transactions:
                if self._all_tx_submitted_event.is_set():
                    print("PoWAdapter: No more pending transactions. Stopping mining.")
                    self._stop_processing_event.set()
                    break
                try:
                    await asyncio.wait_for(asyncio.sleep(0.1), timeout=0.15)
                except asyncio.TimeoutError: pass
                if self._stop_processing_event.is_set(): break
                continue
            tx_list_for_block = list(self.blockchain.pending_transactions)
            print(f"PoWAdapter: Mining block {self.blockchain.last_block.index + 1} with {len(tx_list_for_block)} txs...")
            mined_info = self.pow_consensus.proof_of_work(
                self.blockchain.last_block, tx_list_for_block, miner_id=self.miner_id
            )
            if self._stop_processing_event.is_set(): break
            if mined_info:
                nonce, new_hash, timestamp = mined_info
                new_block = Block(
                    index=self.blockchain.last_block.index + 1,
                    transactions=tx_list_for_block, timestamp=timestamp,
                    previous_hash=self.blockchain.last_block.hash,
                    nonce=nonce, sealer_id=self.miner_id
                )
                if new_block.hash != new_hash:
                    self.metrics_collector.record_error(f"PoWAdapter: Mined hash {new_hash} != block hash {new_block.hash}")
                    continue
                if self.pow_consensus.validate_proof(new_block):
                    if self.blockchain.add_block(new_block):
                        print(f"PoWAdapter: Mined and added Block {new_block.index}. Nonce: {nonce}.")
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
                print("PoWAdapter: Mining returned no result.")
            await asyncio.sleep(0)
        print("PoWAdapter: Mining loop stopped.")

    async def wait_for_completion(self, num_target_transactions: int):
        self._ensure_mining_is_running()
        start_wait = time.perf_counter()
        timeout_base = self.config.get("post_submission_wait_seconds", 20)
        timeout_per_tx = self.config.get("timeout_per_tx_factor_pow", 2.0)
        timeout_seconds = timeout_base + (num_target_transactions * timeout_per_tx)
        print(f"PoWAdapter: Waiting for {num_target_transactions} txs to finalize. Timeout: {timeout_seconds:.2f}s")
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
                self._processing_task.cancel()

class HotStuffAdapter:
    """Adapter for the HotStuff consensus mechanism."""
    def __init__(self, nodes: dict, network_simulator: NetworkSimulator,
                 metrics_collector: MetricsCollector, config: dict):
        self.nodes = nodes
        self.network_simulator = network_simulator
        self.metrics_collector = metrics_collector
        self.config = config
        self._processing_task: asyncio.Task | None = None
        self._stop_processing_event = asyncio.Event()
        self._all_tx_submitted_event = asyncio.Event()
        self.globally_committed_blocks = set()

        self._patch_hotstuff_nodes_for_metrics()

    async def submit_transaction(self, tx: Transaction) -> bool:
        leader_id = None
        if self.nodes:
            any_node_id = list(self.nodes.keys())[0]
            leader_id = self.nodes[any_node_id].primary_id
        if not leader_id:
            self.metrics_collector.record_error("HotStuffAdapter: No leader identified for submission.")
            return False
        leader_node = self.nodes.get(leader_id)
        if leader_node:
            leader_node.handle_client_request(tx.to_dict())
            if self._processing_task is None or self._processing_task.done():
                self._ensure_pacemaker_is_running()
            return True
        return False

    async def no_more_transactions(self):
        self._all_tx_submitted_event.set()

    async def stop_processing(self):
        self._stop_processing_event.set()

    def _ensure_pacemaker_is_running(self):
        if self._processing_task is None or self._processing_task.done():
            print("HotStuffAdapter: Starting pacemaker...")
            self._processing_task = asyncio.create_task(self._pacemaker())

    async def _pacemaker(self):
        """A simplified pacemaker that periodically triggers the leader to propose."""
        beat_interval = self.config.get("hotstuff_beat_interval_seconds", 0.5)
        while not self._stop_processing_event.is_set():
            leader_id = self.nodes[list(self.nodes.keys())[0]].primary_id
            leader_node = self.nodes.get(leader_id)
            if leader_node and leader_node.pending_client_requests:
                leader_node.on_beat()
            await asyncio.sleep(beat_interval)
        print("HotStuffAdapter: Pacemaker stopped.")

    def _patch_hotstuff_nodes_for_metrics(self):
        for node_id, node_obj in self.nodes.items():
            original_execute_block = node_obj.execute_block
            def metrics_execute_block_wrapper(block_hash, _original_method=original_execute_block, _adapter_self=self):
                block_to_execute = _original_method(block_hash)
                if block_to_execute:
                    if block_to_execute.hash not in _adapter_self.globally_committed_blocks:
                        _adapter_self.globally_committed_blocks.add(block_to_execute.hash)
                        global_block_id = f"hotstuff_b{block_to_execute.index}_h{block_to_execute.hash[:6]}"
                        _adapter_self.metrics_collector.record_block_committed(
                            block_id=global_block_id, num_transactions=len(block_to_execute.transactions),
                            timestamp=time.perf_counter()
                        )
                        for tx_dict in block_to_execute.transactions:
                            _adapter_self.metrics_collector.record_tx_finalized(
                                tx_id=tx_dict['transaction_id'], block_id=global_block_id,
                                timestamp=time.perf_counter()
                            )
            node_obj.execute_block = metrics_execute_block_wrapper

    async def wait_for_completion(self, num_target_transactions: int):
        self._ensure_pacemaker_is_running()
        start_wait = time.perf_counter()
        timeout_base = self.config.get("post_submission_wait_seconds", 30)
        timeout_per_tx = self.config.get("timeout_per_tx_factor_hotstuff", 2.5)
        timeout_seconds = timeout_base + (num_target_transactions * timeout_per_tx)
        print(f"HotStuffAdapter: Waiting for {num_target_transactions} txs to finalize. Timeout: {timeout_seconds:.2f}s")
        while True:
            finalized_count = len(self.metrics_collector.tx_finalized_timestamps.keys())
            if finalized_count >= num_target_transactions:
                print(f"HotStuffAdapter: Target of {num_target_transactions} transactions finalized.")
                self._stop_processing_event.set()
                break
            if time.perf_counter() - start_wait > timeout_seconds:
                print(f"HotStuffAdapter: Timeout waiting for completion. {finalized_count}/{num_target_transactions} finalized.")
                self._stop_processing_event.set()
                break
            await asyncio.sleep(0.5)
        if self._processing_task and not self._processing_task.done():
            try:
                await asyncio.wait_for(self._processing_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._processing_task.cancel()

class PoSAdapter:
    """Adapter for the Proof of Stake (PoS) consensus mechanism."""
    def __init__(self, blockchain: Blockchain, validators: dict,
                 metrics_collector: MetricsCollector, config: dict):
        self.blockchain = blockchain
        self.pos_consensus = PoSConsensus(validators)
        self.metrics_collector = metrics_collector
        self.config = config
        self._processing_task: asyncio.Task | None = None
        self._stop_processing_event = asyncio.Event()
        self._all_tx_submitted_event = asyncio.Event()
        self._creation_lock = asyncio.Lock()

    async def submit_transaction(self, tx: Transaction) -> bool:
        self.blockchain.add_transaction(tx)
        async with self._creation_lock:
            self._ensure_block_creation_is_running()
        return True

    async def no_more_transactions(self):
        self._all_tx_submitted_event.set()

    async def stop_processing(self):
        self._stop_processing_event.set()

    def _ensure_block_creation_is_running(self):
        if self._processing_task is None or self._processing_task.done():
            print("PoSAdapter: Starting block creation process...")
            self._processing_task = asyncio.create_task(self._create_blocks())

    async def _create_blocks(self):
        """Internal task that simulates a validator creating blocks periodically."""
        block_interval = self.config.get("pos_block_interval_seconds", 1.0)
        while not self._stop_processing_event.is_set():
            if not self.blockchain.pending_transactions:
                if self._all_tx_submitted_event.is_set():
                    print("PoSAdapter: All submitted transactions processed. Stopping block creation.")
                    self._stop_processing_event.set()
                    break
                try:
                    await asyncio.wait_for(asyncio.sleep(0.1), timeout=0.15)
                except asyncio.TimeoutError: pass
                if self._stop_processing_event.is_set(): break
                continue

            tx_list_for_block = list(self.blockchain.pending_transactions)
            new_block = self.pos_consensus.create_block(self.blockchain.last_block, tx_list_for_block)

            if self.pos_consensus.validate_block(new_block, self.blockchain.last_block):
                if self.blockchain.add_block(new_block):
                    print(f"PoSAdapter: Block {new_block.index} created by {new_block.sealer_id} with {len(new_block.transactions)} txs.")
                    self.metrics_collector.record_block_committed(
                        new_block.index, len(new_block.transactions), time.perf_counter()
                    )
                    for tx_dict in new_block.transactions:
                        self.metrics_collector.record_tx_finalized(
                            tx_dict['transaction_id'], new_block.index, time.perf_counter()
                        )
                else:
                    self.metrics_collector.record_error(f"PoSAdapter: Blockchain rejected valid PoS block {new_block.index}")
            else:
                self.metrics_collector.record_error(f"PoSAdapter: Created PoS block {new_block.index} failed validation.")

            try:
                await asyncio.wait_for(asyncio.sleep(block_interval), timeout=block_interval + 0.05)
            except asyncio.TimeoutError: pass
            if self._stop_processing_event.is_set(): break

        print("PoSAdapter: Block creation loop stopped.")

    async def wait_for_completion(self, num_target_transactions: int):
        self._ensure_block_creation_is_running()
        start_wait = time.perf_counter()
        timeout_base = self.config.get("post_submission_wait_seconds", 20)
        timeout_per_tx = self.config.get("timeout_per_tx_factor_pos", 1.5)
        timeout_seconds = timeout_base + (num_target_transactions * timeout_per_tx)

        print(f"PoSAdapter: Waiting for {num_target_transactions} txs to finalize. Timeout: {timeout_seconds:.2f}s")

        while not self._stop_processing_event.is_set():
            finalized_count = len(self.metrics_collector.tx_finalized_timestamps.keys())
            if finalized_count >= num_target_transactions:
                print(f"PoSAdapter: Target of {num_target_transactions} transactions finalized.")
                self._stop_processing_event.set()
                break
            if time.perf_counter() - start_wait > timeout_seconds:
                print(f"PoSAdapter: Timeout waiting for completion. {finalized_count}/{num_target_transactions} finalized.")
                self._stop_processing_event.set()
                break
            await asyncio.sleep(0.2)

        if self._processing_task and not self._processing_task.done():
            try:
                await asyncio.wait_for(self._processing_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._processing_task.cancel()

class DPoSAdapter:
    """
    Adapter for the Delegated Proof of Stake (DPoS) consensus mechanism.
    Simulates block creation by a rotating schedule of pre-defined delegates.
    """
    def __init__(self, blockchain: Blockchain, delegates: list,
                 metrics_collector: MetricsCollector, config: dict):
        self.blockchain = blockchain
        self.dpos_consensus = DPoSConsensus(delegates)
        self.metrics_collector = metrics_collector
        self.config = config
        self._processing_task: asyncio.Task | None = None
        self._stop_processing_event = asyncio.Event()
        self._all_tx_submitted_event = asyncio.Event()
        self._creation_lock = asyncio.Lock()

    async def submit_transaction(self, tx: Transaction) -> bool:
        """Adds transaction to the blockchain's pending pool."""
        self.blockchain.add_transaction(tx)
        async with self._creation_lock:
            self._ensure_block_creation_is_running()
        return True

    async def no_more_transactions(self):
        self._all_tx_submitted_event.set()

    async def stop_processing(self):
        self._stop_processing_event.set()

    def _ensure_block_creation_is_running(self):
        if self._processing_task is None or self._processing_task.done():
            print("DPoSAdapter: Starting block creation process...")
            self._processing_task = asyncio.create_task(self._create_blocks())

    async def _create_blocks(self):
        """
        Internal task that simulates a delegate creating a block when it's their turn.
        """
        block_interval = self.config.get("dpos_block_interval_seconds", 0.5) # DPoS often has fast, regular blocks
        while not self._stop_processing_event.is_set():
            if not self.blockchain.pending_transactions:
                if self._all_tx_submitted_event.is_set():
                    print("DPoSAdapter: All submitted transactions processed. Stopping block creation.")
                    self._stop_processing_event.set()
                    break
                try:
                    await asyncio.wait_for(asyncio.sleep(0.1), timeout=0.15)
                except asyncio.TimeoutError: pass
                if self._stop_processing_event.is_set(): break
                continue

            tx_list_for_block = list(self.blockchain.pending_transactions)

            # create_block in DPoSConsensus handles delegate selection and block creation
            new_block = self.dpos_consensus.create_block(self.blockchain.last_block, tx_list_for_block)

            if self.dpos_consensus.validate_block(new_block):
                if self.blockchain.add_block(new_block):
                    print(f"DPoSAdapter: Block {new_block.index} created by {new_block.sealer_id} with {len(new_block.transactions)} txs.")
                    self.metrics_collector.record_block_committed(
                        new_block.index, len(new_block.transactions), time.perf_counter()
                    )
                    for tx_dict in new_block.transactions:
                        self.metrics_collector.record_tx_finalized(
                            tx_dict['transaction_id'], new_block.index, time.perf_counter()
                        )
                else:
                    self.metrics_collector.record_error(f"DPoSAdapter: Blockchain rejected valid DPoS block {new_block.index}")
            else:
                # This should not happen if our own delegate created it correctly
                self.metrics_collector.record_error(f"DPoSAdapter: Created DPoS block {new_block.index} failed validation.")

            try:
                await asyncio.wait_for(asyncio.sleep(block_interval), timeout=block_interval + 0.05)
            except asyncio.TimeoutError: pass
            if self._stop_processing_event.is_set(): break

        print("DPoSAdapter: Block creation loop stopped.")

    async def wait_for_completion(self, num_target_transactions: int):
        self._ensure_block_creation_is_running()
        start_wait = time.perf_counter()
        timeout_base = self.config.get("post_submission_wait_seconds", 15)
        timeout_per_tx = self.config.get("timeout_per_tx_factor_dpos", 1.0)
        timeout_seconds = timeout_base + (num_target_transactions * timeout_per_tx)

        print(f"DPoSAdapter: Waiting for {num_target_transactions} txs to finalize. Timeout: {timeout_seconds:.2f}s")

        while not self._stop_processing_event.is_set():
            finalized_count = len(self.metrics_collector.tx_finalized_timestamps.keys())
            if finalized_count >= num_target_transactions:
                print(f"DPoSAdapter: Target of {num_target_transactions} transactions finalized.")
                self._stop_processing_event.set()
                break
            if time.perf_counter() - start_wait > timeout_seconds:
                print(f"DPoSAdapter: Timeout waiting for completion. {finalized_count}/{num_target_transactions} finalized.")
                self._stop_processing_event.set()
                break
            await asyncio.sleep(0.2)

        if self._processing_task and not self._processing_task.done():
            try:
                await asyncio.wait_for(self._processing_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._processing_task.cancel()
