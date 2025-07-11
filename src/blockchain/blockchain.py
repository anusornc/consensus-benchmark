import time
import json # For precise removal of transactions
from .block import Block
from src.transactions.transaction import Transaction # For type hinting if constructing directly

class Blockchain:
    """
    Manages the chain of blocks, pending transactions, and blockchain validation.

    Attributes:
        chain (list[Block]): The list of blocks forming the blockchain.
        pending_transactions (list[dict]): Transactions waiting to be included in a block.
                                           Stored as dictionaries (from Transaction.to_dict()).
        genesis_sealer_id (str): Identifier for the sealer of the genesis block.
    """
    def __init__(self, genesis_sealer_id: str = "genesis_system_sealer"):
        """
        Initializes the Blockchain, creating the genesis block.

        Args:
            genesis_sealer_id (str, optional): The sealer ID for the genesis block.
                                               Defaults to "genesis_system_sealer".
        """
        self.chain: list[Block] = []
        self.pending_transactions: list[dict] = []
        self.genesis_sealer_id = genesis_sealer_id
        self._create_genesis_block()

    def _create_genesis_block(self):
        """
        Creates the first block in the blockchain (the Genesis block).
        This is an internal method called during initialization.
        """
        # Create a dummy transaction for genesis block using Transaction.to_dict()
        # This requires Transaction class to be importable and instantiable.
        # If Transaction class itself has complex dependencies for PROV-O,
        # we might need a simpler way to make a genesis transaction dict.
        # For now, assume Transaction can be instantiated simply for this purpose,
        # or we directly craft the dict.
        try:
            genesis_tx_payload = Transaction(
                transaction_type="system_genesis",
                asset_id_value="genesis_asset",
                agent_ids={"creator_agent_id": "system"}
            ).to_dict()
        except Exception as e:
            # Fallback if full Transaction init is problematic here
            print(f"Warning: Could not create full PROV-O genesis transaction due to: {e}. Using simplified genesis tx.")
            genesis_tx_payload = {
                "transaction_id": f"urn:uuid:{uuid4()}", "timestamp": time.time(),
                "transaction_type": "system_genesis", "asset_id_value": "genesis_asset",
                "agent_ids": {"creator_agent_id": "system"},
                "data": {"@context": {}, "@graph": [{"@id": "urn:example:genesis", "message": "Genesis Block"}]}
            }

        genesis_block = Block(
            index=0,
            transactions=[genesis_tx_payload], # Store as list of dicts
            timestamp=time.time(),
            previous_hash="0",
            sealer_id=self.genesis_sealer_id
        )
        self.chain.append(genesis_block)

    @property
    def last_block(self) -> Block | None:
        """
        Returns the last block in the chain, or None if the chain is empty.
        """
        if not self.chain:
            return None
        return self.chain[-1]

    def add_transaction(self, transaction: Transaction) -> int:
        """
        Adds a new transaction to the list of pending transactions.
        The transaction is stored as its dictionary representation.

        Args:
            transaction (Transaction): The transaction object to add.

        Returns:
            int: The index of the block where this transaction might be included.
        """
        self.pending_transactions.append(transaction.to_dict())
        last_b = self.last_block
        return last_b.index + 1 if last_b else 0 # Potential next block index


    def add_block(self, block: Block) -> bool:
        """
        Adds a pre-constructed and consensus-validated block to the blockchain.

        This method performs basic validation checks:
        1. Correct linkage: `block.previous_hash` must match `self.last_block.hash`.
        2. Sequential index: `block.index` must be `self.last_block.index + 1`.
        3. Integrity of the block itself: `block.hash` must match `block.calculate_hash()`.

        If validation passes, the block is appended to the chain, and transactions
        included in the block are removed from the `pending_transactions` pool.

        Args:
            block (Block): The block to add. It's assumed to be validated by a
                           consensus mechanism before being passed here.

        Returns:
            bool: True if the block was added successfully, False otherwise.
        """
        current_last_block = self.last_block
        if not current_last_block: # Should only happen if chain was somehow cleared post-init
            if block.index == 0 and block.previous_hash == "0": # Allowing re-adding a genesis block
                if block.hash != block.calculate_hash():
                    print(f"Error adding genesis: Block's hash is invalid. Expected {block.calculate_hash()}, got {block.hash}.")
                    return False
                self.chain.append(block)
                self._remove_transactions_from_pending(block.transactions)
                return True
            print("Error: Blockchain is unexpectedly empty (no genesis block), cannot add non-genesis block.")
            return False

        if current_last_block.hash != block.previous_hash:
            print(f"Error: New block's previous_hash ({block.previous_hash[:8]}...) does not match current last block's hash ({current_last_block.hash[:8]}...).")
            return False
        if block.index != current_last_block.index + 1:
            print(f"Error: New block's index ({block.index}) is not sequential (expected {current_last_block.index + 1}).")
            return False
        if block.hash != block.calculate_hash(): # Verifies block's self-integrity
            print(f"Error: New block's own hash is invalid. Expected {block.calculate_hash()[:8]}..., got {block.hash[:8]}....")
            return False

        self.chain.append(block)
        self._remove_transactions_from_pending(block.transactions)
        return True

    def _remove_transactions_from_pending(self, block_transactions: list[dict]):
        """
        Removes transactions that are now included in a block from the pending transactions list.

        Comparison is based on the JSON string representation of transaction dictionaries
        (sorted keys) to ensure that logically identical transactions are matched even if
        their in-memory dictionary key order differs.

        Args:
            block_transactions (list[dict]): A list of transaction dictionaries from the added block.
        """
        # Create a set of JSON strings for efficient lookup of transactions in the block
        block_tx_json_set = set()
        for tx_dict in block_transactions:
            try:
                block_tx_json_set.add(json.dumps(tx_dict, sort_keys=True))
            except TypeError as e:
                # This might happen if a transaction dict is malformed or contains non-serializable elements.
                print(f"Warning: Could not serialize transaction from block for removal comparison: {str(tx_dict)[:100]}. Error: {e}")


        new_pending_transactions = []
        for pt_dict in self.pending_transactions:
            try:
                pt_json = json.dumps(pt_dict, sort_keys=True)
                if pt_json not in block_tx_json_set:
                    new_pending_transactions.append(pt_dict)
            except TypeError as e:
                print(f"Warning: Could not serialize pending transaction for comparison: {str(pt_dict)[:100]}. Error: {e}")
                new_pending_transactions.append(pt_dict) # Keep if unsure, to avoid data loss
        self.pending_transactions = new_pending_transactions


    def is_chain_valid(self, chain_to_validate: list[Block] = None) -> bool:
        """
        Validates the integrity of the entire blockchain or a given chain segment.

        Checks performed:
        - Genesis block: Must have index 0, `previous_hash` of "0", and a valid self-hash.
        - Subsequent blocks:
            - Index must be sequential (previous block's index + 1).
            - `previous_hash` must match the hash of the actual previous block.
            - Self-hash must be correctly calculated from its content.

        Args:
            chain_to_validate (list[Block], optional): An external chain to validate.
                                                       If None, validates `self.chain`.

        Returns:
            bool: True if the chain is valid, False otherwise.
        """
        current_chain = chain_to_validate if chain_to_validate is not None else self.chain

        if not current_chain:
            # An empty chain might be considered valid in some contexts, or invalid if genesis is expected.
            # Given our __init__ creates a genesis, an empty self.chain post-init implies an issue.
            print("Warning: Validating an empty chain.")
            return True

        # Check genesis block
        first_block = current_chain[0]
        if first_block.index != 0:
            print(f"Genesis block invalid: Index {first_block.index} is not 0.")
            return False
        if first_block.previous_hash != "0":
            print(f"Genesis block invalid: Previous_hash '{first_block.previous_hash}' is not '0'.")
            return False
        if first_block.hash != first_block.calculate_hash():
            print(f"Genesis block invalid: Hash mismatch. Calculated {first_block.calculate_hash()[:8]}..., stored {first_block.hash[:8]}....")
            return False

        # Check rest of the chain
        for i in range(1, len(current_chain)):
            current_block = current_chain[i]
            previous_block = current_chain[i-1]

            if current_block.index != previous_block.index + 1:
                print(f"Block {current_block.index}: Invalid index. Expected {previous_block.index + 1}.")
                return False
            if current_block.previous_hash != previous_block.hash:
                print(f"Block {current_block.index} (Hash: {current_block.hash[:8]}...): Previous hash mismatch. Expected {previous_block.hash[:8]}..., got {current_block.previous_hash[:8]}....")
                return False
            if current_block.hash != current_block.calculate_hash():
                print(f"Block {current_block.index}: Hash calculation incorrect. Expected {current_block.calculate_hash()[:8]}..., got {current_block.hash[:8]}....")
                return False
        return True

    def __repr__(self) -> str:
        """
        Returns a string representation of the Blockchain.
        """
        return f"Blockchain (Blocks: {len(self.chain)}, Pending Transactions: {len(self.pending_transactions)})"

if __name__ == '__main__':
    # Example Usage with the new add_block logic
    # Note: Transaction class needs to be accessible for this __main__ to run as is.
    # Assuming 'from src.transactions.transaction import Transaction' works if run from project root.
    # If running this file directly, imports might need adjustment or Transaction needs to be defined/mocked here.

    # To make __main__ runnable standalone for quick tests, let's use a mock Transaction if real one fails to import
    try:
        from src.transactions.transaction import Transaction
    except ImportError:
        print("Warning: Real Transaction class not found, using a mock for Blockchain __main__.")
        class Transaction: # Basic Mock for testing Blockchain standalone
            def __init__(self, transaction_type, asset_id_value, agent_ids, **kwargs):
                self.transaction_id = f"mock_tx_{uuid4()}"
                self.timestamp = time.time()
                self.type = transaction_type
                self.asset_id = asset_id_value
                self.agents = agent_ids
                self.data = {"mock_data": True, **kwargs}
            def to_dict(self):
                return {"transaction_id": self.transaction_id, "timestamp": self.timestamp,
                        "type": self.type, "asset_id": self.asset_id, "agents": self.agents, "data": self.data}


    blockchain = Blockchain(genesis_sealer_id="system_genesis_sealer_main")
    print(blockchain)
    print(f"Genesis Block: {blockchain.last_block}")

    # Add some transactions
    tx1 = Transaction(transaction_type="asset_creation", asset_id_value="asset001", agent_ids={"creator_agent_id":"Alice"})
    tx2 = Transaction(transaction_type="asset_transfer", asset_id_value="asset001", agent_ids={"from_agent_id":"Alice", "to_agent_id":"Bob"})

    blockchain.add_transaction(tx1)
    blockchain.add_transaction(tx2)
    print(f"Pending transactions ({len(blockchain.pending_transactions)}): {str(blockchain.pending_transactions)[:200]}...")


    if blockchain.pending_transactions:
        selected_txs_for_block1 = list(blockchain.pending_transactions)

        block1_manual_data = {
            "index": blockchain.last_block.index + 1,
            "transactions": selected_txs_for_block1, # These are dicts
            "timestamp": time.time(),
            "previous_hash": blockchain.last_block.hash,
            "nonce": 12345,
            "sealer_id": "authority_A"
        }
        block1_manual = Block(**block1_manual_data)

        print(f"\nAttempting to add manually created Block 1 (Sealer: {block1_manual.sealer_id})...")
        if blockchain.add_block(block1_manual):
            print(f"New Block added: {block1_manual}")
            print(blockchain)
            print(f"Pending transactions after block add: {len(blockchain.pending_transactions)} (Expected empty if all taken)")
            assert not blockchain.pending_transactions, "Pending transactions should be empty"
        else:
            print(f"Failed to add block1_manual.")

    print(f"\nIs chain valid after Block 1? {blockchain.is_chain_valid()}")

    # Add more transactions and another block
    tx3 = Transaction(transaction_type="asset_update", asset_id_value="asset001", agent_ids={"modifier_agent_id":"Bob"}, ex_property="new_value")
    blockchain.add_transaction(tx3)

    if blockchain.pending_transactions:
        selected_txs_for_block2 = list(blockchain.pending_transactions)
        block2_manual_data = {
            "index": blockchain.last_block.index + 1,
            "transactions": selected_txs_for_block2,
            "timestamp": time.time(),
            "previous_hash": blockchain.last_block.hash,
            "nonce": 54321,
            "sealer_id": "authority_B"
        }
        block2_manual = Block(**block2_manual_data)

        print(f"\nAttempting to add manually created Block 2 (Sealer: {block2_manual.sealer_id})...")
        if blockchain.add_block(block2_manual):
            print(f"New Block added: {block2_manual}")
            print(blockchain)
        else:
            print(f"Failed to add block2_manual.")

    print(f"\nIs chain valid after Block 2? {blockchain.is_chain_valid()}")

    # Tamper with the chain to test validation
    if len(blockchain.chain) > 1:
        print("\n--- Tampering Test ---")
        original_block1_txs = list(blockchain.chain[1].transactions) # Save for restoration

        print(f"Original Block 1: {blockchain.chain[1]}")
        # Tamper transactions in block 1
        try:
            hacker_tx = Transaction(transaction_type="malicious_tx", asset_id_value="hacked_asset", agent_ids={"hacker":"evil_corp"}).to_dict()
        except NameError: # If real Transaction class failed import, use simple dict
             hacker_tx = {"transaction_id": "hacker_tx", "data": "malicious data"}

        blockchain.chain[1].transactions = [hacker_tx]

        print(f"Is chain valid after tampering Block 1's transactions (hash field NOT updated)? {blockchain.is_chain_valid()} (Expected False)")
        assert not blockchain.is_chain_valid()

        blockchain.chain[1].hash = blockchain.chain[1].calculate_hash() # Hacker recalculates hash of tampered block
        print(f"Updated Block 1 Hash after tampering: {blockchain.chain[1].hash[:8]}...")
        print(f"Is chain valid after tampering Block 1 (tx & its hash field recalculated)? {blockchain.is_chain_valid()} (Expected False because Block 2 link is broken)")
        assert not blockchain.is_chain_valid()

        # Restore block 1 for further tests (simplified restoration)
        blockchain.chain[1].transactions = original_block1_txs
        blockchain.chain[1].hash = blockchain.chain[1].calculate_hash()
        # Note: This only fixes block 1. If block 2 existed, the chain is still broken there.
        # For robust testing after tampering, re-initialize or fix subsequent blocks too.
        print(f"Is chain valid after restoring Block 1 (Block 2 link still broken if it exists)? {blockchain.is_chain_valid()}")


    print("\n--- Genesis Validation Tests (on fresh chains) ---")
    bc_test_genesis_idx = Blockchain(genesis_sealer_id="g_idx_main")
    bc_test_genesis_idx.chain[0].index = 10
    print(f"Is chain valid after tampering genesis index? {bc_test_genesis_idx.is_chain_valid()} (Expected False)")
    assert not bc_test_genesis_idx.is_chain_valid()

    bc_test_genesis_phash = Blockchain(genesis_sealer_id="g_phash_main")
    bc_test_genesis_phash.chain[0].previous_hash = "123"
    print(f"Is chain valid after tampering genesis previous_hash? {bc_test_genesis_phash.is_chain_valid()} (Expected False)")
    assert not bc_test_genesis_phash.is_chain_valid()

    bc_test_genesis_hash_self = Blockchain(genesis_sealer_id="g_hself_main")
    bc_test_genesis_hash_self.chain[0].hash = "tampered_genesis_hash_value"
    print(f"Is chain valid after tampering genesis self-hash? {bc_test_genesis_hash_self.is_chain_valid()} (Expected False)")
    assert not bc_test_genesis_hash_self.is_chain_valid()


    # Re-init blockchain for add_block failure tests
    blockchain_for_add_fail_test = Blockchain(genesis_sealer_id="add_fail_genesis")
    # Add one valid block to have a last_block.hash
    valid_tx_for_setup = Transaction(transaction_type="setup", asset_id_value="setup_asset", agent_ids={"creator":"test_setup"}).to_dict()
    setup_block_data = {"index":1, "transactions":[valid_tx_for_setup], "timestamp":time.time(),
                        "previous_hash":blockchain_for_add_fail_test.last_block.hash, "nonce":1, "sealer_id":"setup_sealer"}
    setup_block = Block(**setup_block_data)
    blockchain_for_add_fail_test.add_block(setup_block)
    assert blockchain_for_add_fail_test.is_chain_valid()
    print(f"\nBlockchain for add_block failure tests has {len(blockchain_for_add_fail_test.chain)} blocks and is valid.")

    print("\n--- add_block Failure Tests ---")
    dummy_tx_list = [Transaction(transaction_type="dummy", asset_id_value="dummy_asset", agent_ids={"creator":"dummy_creator"}).to_dict()]

    # Test adding a block with wrong previous_hash
    bad_prev_hash_block_data = {"index":blockchain_for_add_fail_test.last_block.index + 1, "transactions":dummy_tx_list, "timestamp":time.time(),
                                "previous_hash":"incorrect_previous_hash_value", "nonce":0, "sealer_id":"any_sealer"}
    bad_prev_hash_block = Block(**bad_prev_hash_block_data)
    print(f"Attempting to add block with wrong previous_hash...")
    if not blockchain_for_add_fail_test.add_block(bad_prev_hash_block):
        print("Correctly failed to add block with bad previous_hash.")
    else:
        print("ERROR: Added block with bad previous_hash.")
        assert False, "Test failed: Added block with bad previous_hash"
    assert blockchain_for_add_fail_test.is_chain_valid(), "Chain should remain valid after failed add"

    # Test adding a block with wrong index
    bad_index_block_data = {"index":blockchain_for_add_fail_test.last_block.index + 5, "transactions":dummy_tx_list, "timestamp":time.time(),
                            "previous_hash":blockchain_for_add_fail_test.last_block.hash, "nonce":0, "sealer_id":"any_sealer"}
    bad_index_block = Block(**bad_index_block_data)
    print(f"Attempting to add block with wrong index...")
    if not blockchain_for_add_fail_test.add_block(bad_index_block):
        print("Correctly failed to add block with bad index.")
    else:
        print("ERROR: Added block with bad index.")
        assert False, "Test failed: Added block with bad index"
    assert blockchain_for_add_fail_test.is_chain_valid(), "Chain should remain valid after failed add"

    # Test adding a block with invalid self-hash (block.hash manipulated after creation)
    invalid_self_hash_block_data = {"index":blockchain_for_add_fail_test.last_block.index + 1, "transactions":dummy_tx_list, "timestamp":time.time(),
                                    "previous_hash":blockchain_for_add_fail_test.last_block.hash, "nonce":0, "sealer_id":"any_sealer"}
    invalid_self_hash_block = Block(**invalid_self_hash_block_data)
    invalid_self_hash_block.hash = "totally_wrong_hash_value_manipulated" # Manipulate after construction
    print(f"Attempting to add block with invalid self-hash...")
    if not blockchain_for_add_fail_test.add_block(invalid_self_hash_block):
        print("Correctly failed to add block with invalid self-hash.")
    else:
        print("ERROR: Added block with invalid self-hash.")
        assert False, "Test failed: Added block with invalid self-hash"
    assert blockchain_for_add_fail_test.is_chain_valid(), "Chain should remain valid after failed add"

    print("\nBlockchain Class __main__ tests completed.")
