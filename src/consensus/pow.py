import hashlib
import json
import time
from src.blockchain.block import Block # For type hinting and potentially for validation

class PoWConsensus:
    """
    Implements a basic Proof of Work (PoW) consensus mechanism.

    Attributes:
        difficulty (int): The number of leading zeros required in a block hash
                          for it to be considered valid (proof of work).
    """
    def __init__(self, difficulty: int = 4):
        """
        Initializes the PoWConsensus mechanism.

        Args:
            difficulty (int, optional): The number of leading zeros for the PoW target.
                                        Defaults to 4. Higher numbers increase mining time.
        """
        if difficulty < 1:
            raise ValueError("Difficulty must be at least 1.")
        self.difficulty = difficulty
        self.target_prefix = '0' * self.difficulty
        print(f"PoWConsensus initialized with difficulty {self.difficulty} (target prefix: '{self.target_prefix}')")

    def _calculate_hash_for_pow(self, index: int, transactions: list, timestamp: float,
                                previous_hash: str, nonce: int, sealer_id: str = None) -> str:
        """
        Helper to calculate a hash for PoW purposes based on block components.
        This is similar to Block.calculate_hash but allows direct input of components.
        """
        block_dict = {
            'index': index,
            'transactions': transactions,
            'timestamp': timestamp,
            'previous_hash': previous_hash,
            'nonce': nonce,
            'sealer_id': sealer_id # PoW doesn't typically use sealer_id, but Block class includes it
        }
        block_string = json.dumps(block_dict, sort_keys=True).encode('utf-8')
        return hashlib.sha256(block_string).hexdigest()

    def proof_of_work(self, last_block: Block, transactions: list[dict],
                      miner_id: str = "miner_pow") -> tuple[int, str, float] | None:
        """
        Performs the mining process to find a nonce that results in a block hash
        meeting the current difficulty target.

        Args:
            last_block (Block): The last block in the current blockchain.
                                Used to get `previous_hash` and `index` for the new block.
            transactions (list[dict]): The list of transactions to include in the new block.
            miner_id (str, optional): Identifier for the miner (can be used as sealer_id).
                                      Defaults to "miner_pow".

        Returns:
            tuple[int, str, float] | None: A tuple containing (nonce, new_hash, timestamp) if a valid nonce is found.
                                 Returns None if interrupted or fails (not implemented here).
                                 The timestamp is the time when the proof was found.
        """
        new_block_index = last_block.index + 1
        previous_hash = last_block.hash
        nonce = 0

        print(f"PoW: Mining block {new_block_index} with difficulty {self.difficulty} (prefix: '{self.target_prefix}')...")
        start_time = time.time()

        while True: # Potentially add a timeout or max_nonce for practical limits
            timestamp = time.time() # Timestamp for each attempt, or one at the start of mining
            attempted_hash = self._calculate_hash_for_pow(
                index=new_block_index,
                transactions=transactions,
                timestamp=timestamp,
                previous_hash=previous_hash,
                nonce=nonce,
                sealer_id=miner_id # Using miner_id as sealer for consistency with Block structure
            )
            if attempted_hash.startswith(self.target_prefix):
                end_time = time.time()
                print(f"PoW: Mined block {new_block_index}! Nonce: {nonce}, Hash: {attempted_hash[:10]}... (took {end_time - start_time:.2f}s)")
                return nonce, attempted_hash, timestamp

            nonce += 1
            if nonce % 100000 == 0: # Provide some feedback for long mining sessions
                print(f"PoW: Still mining block {new_block_index}... (nonce: {nonce})")


    def validate_proof(self, block: Block) -> bool:
        """
        Validates if a given block's hash meets the difficulty target and is correctly calculated.

        Args:
            block (Block): The block to validate.

        Returns:
            bool: True if the block's proof of work is valid, False otherwise.
        """
        if block.hash != block.calculate_hash():
            print(f"PoW Validate: Block {block.index} hash mismatch. Stored: {block.hash}, Calculated: {block.calculate_hash()}")
            return False
        if not block.hash.startswith(self.target_prefix):
            print(f"PoW Validate: Block {block.index} hash {block.hash[:10]}... does not meet target prefix '{self.target_prefix}'.")
            return False

        # print(f"PoW Validate: Block {block.index} proof is valid.")
        return True

    # Placeholder for difficulty adjustment logic
    def adjust_difficulty(self, blockchain_instance, expected_block_time: int = 10) -> int:
        """
        Adjusts the mining difficulty based on the time taken for recent blocks.
        (This is a simplified placeholder and not fully implemented for dynamic adjustment).

        Args:
            blockchain_instance: The current blockchain, to access recent block times.
            expected_block_time (int, optional): The target average time in seconds per block.

        Returns:
            int: The new difficulty level. Currently returns existing difficulty.
        """
        # Basic idea:
        # If len(blockchain_instance.chain) > 1:
        #     last_block = blockchain_instance.last_block
        #     second_last_block = blockchain_instance.chain[-2]
        #     time_diff = last_block.timestamp - second_last_block.timestamp
        #     if time_diff < expected_block_time / 2: # Too fast
        #         self.difficulty += 1
        #     elif time_diff > expected_block_time * 2: # Too slow
        #         self.difficulty = max(1, self.difficulty - 1)
        #     self.target_prefix = '0' * self.difficulty
        #     print(f"PoW: Difficulty adjusted to {self.difficulty}")
        print(f"PoW: Difficulty adjustment not fully implemented. Current difficulty: {self.difficulty}")
        return self.difficulty


if __name__ == '__main__':
    print("--- PoW Consensus Basic Test ---")

    # Mock a last block (like a genesis block)
    mock_genesis_txs = [{"tx_id": "genesis", "data": "mock genesis"}]
    mock_last_block = Block(0, mock_genesis_txs, time.time() - 10, "0", 0, "system")

    difficulty = 4 # Number of leading zeros
    pow_consensus = PoWConsensus(difficulty=difficulty)

    # Transactions for the new block
    new_transactions = [
        Transaction(transaction_type="test_tx", asset_id_value="asset123", agent_ids={"creator_agent_id":"test_user"}).to_dict()
    ]

    # Mine a new block
    mined_info = pow_consensus.proof_of_work(mock_last_block, new_transactions, miner_id="test_miner")

    if mined_info:
        found_nonce, found_hash, found_timestamp = mined_info

        # Create the block with the found nonce
        newly_mined_block = Block(
            index=mock_last_block.index + 1,
            transactions=new_transactions,
            timestamp=found_timestamp,
            previous_hash=mock_last_block.hash,
            nonce=found_nonce,
            sealer_id="test_miner", # Could be miner's ID
        )
        # The block's hash will be re-calculated on init, it should match found_hash
        assert newly_mined_block.hash == found_hash, "Block hash upon creation differs from mined hash!"

        print(f"\nNewly Mined Block: {newly_mined_block}")

        # Validate the proof
        is_valid = pow_consensus.validate_proof(newly_mined_block)
        print(f"Validation of mined block's proof: {is_valid}")
        assert is_valid, "Mined block failed validation!"

        # Test validation with a block that shouldn't pass
        invalid_block = Block(
            index=mock_last_block.index + 1,
            transactions=new_transactions,
            timestamp=time.time(),
            previous_hash=mock_last_block.hash,
            nonce=123, # Arbitrary nonce, likely won't meet difficulty
            sealer_id="test_miner"
        )
        print(f"\nInvalid Block (for testing validation): {invalid_block}")
        is_invalid_proof_valid = pow_consensus.validate_proof(invalid_block)
        print(f"Validation of invalid block's proof: {is_invalid_proof_valid}")
        assert not is_invalid_proof_valid, "Invalid block unexpectedly passed validation!"

        # Test with different difficulty
        print("\n--- Testing with higher difficulty (may take longer) ---")
        # pow_consensus_hard = PoWConsensus(difficulty=5)
        # mined_info_hard = pow_consensus_hard.proof_of_work(newly_mined_block, [{"data":"another tx"}])
        # if mined_info_hard:
        #     print(f"Mined with higher difficulty: Nonce {mined_info_hard[0]}, Hash {mined_info_hard[1][:10]}...")
        # else:
        #     print("Mining with higher difficulty did not complete in this simple test.")
        print("(Skipping higher difficulty test in __main__ for speed)")


    else:
        print("Mining failed (this should not happen in this basic test unless interrupted).")

    print("\nPoW Consensus basic tests finished.")
