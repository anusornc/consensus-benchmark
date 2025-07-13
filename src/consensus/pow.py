import hashlib
import json
import time
from src.blockchain.block import Block
from src.transactions.transaction import Transaction

class PoWConsensus:
    """
    Implements a basic Proof of Work (PoW) consensus mechanism.

    Attributes:
        difficulty (int): The number of leading zeros required in a block hash
                          for it to be considered valid (proof of work).
    """
    def __init__(self, difficulty: int = 2):
        """
        Initializes the PoWConsensus mechanism.

        Args:
            difficulty (int, optional): The number of leading zeros for the PoW target.
                                        Defaults to 2. Higher numbers increase mining time.
        """
        if difficulty < 1:
            raise ValueError("Difficulty must be at least 1.")
        self.difficulty = difficulty
        self.target_prefix = '0' * self.difficulty
        print(f"PoWConsensus initialized with difficulty {self.difficulty} (target prefix: '{self.target_prefix}')")

    def proof_of_work(self, last_block: Block, transactions: list[dict], miner_id: str = "miner_pow") -> tuple[int, str, float] | None:
        """
        Performs the mining process to find a nonce that results in a block hash
        meeting the current difficulty target. Uses a fixed timestamp for the block.
        Adds a nonce limit to prevent infinite loops.
        """
        new_block_index = last_block.index + 1
        previous_hash = last_block.hash
        nonce = 0
        max_nonce = 1000000  # Safety limit
        timestamp = time.time()  # Fixed timestamp for the block

        print(f"PoW: Mining block {new_block_index} with difficulty {self.difficulty} (prefix: '{self.target_prefix}')...")
        start_time = time.time()

        while nonce < max_nonce:
            # Create a temporary block and use Block's hash method to ensure consistency
            temp_block = Block(
                index=new_block_index,
                transactions=transactions,
                timestamp=timestamp,
                previous_hash=previous_hash,
                nonce=nonce,
                sealer_id=miner_id
            )
            attempted_hash = temp_block.calculate_hash()
            
            if attempted_hash.startswith(self.target_prefix):
                end_time = time.time()
                print(f"PoW: Mined block {new_block_index}! Nonce: {nonce}, Hash: {attempted_hash[:10]}... (took {end_time - start_time:.2f}s)")
                return nonce, attempted_hash, timestamp

            nonce += 1
            if nonce % 10000 == 0:  # Reduced frequency for less spam
                print(f"PoW: Still mining block {new_block_index}... (nonce: {nonce})")

        print("PoW: Mining timeout - no solution found within nonce limit.")
        return None

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
        # Basic idea for future implementation:
        # if len(blockchain_instance.chain) > 1:
        #     last_block = blockchain_instance.last_block
        #     second_last_block = blockchain_instance.chain[-2]
        #     time_diff = last_block.timestamp - second_last_block.timestamp
        #     if time_diff < expected_block_time / 2:  # Too fast
        #         self.difficulty += 1
        #     elif time_diff > expected_block_time * 2:  # Too slow
        #         self.difficulty = max(1, self.difficulty - 1)
        #     self.target_prefix = '0' * self.difficulty
        #     print(f"PoW: Difficulty adjusted to {self.difficulty}")
        print(f"PoW: Difficulty adjustment not fully implemented. Current difficulty: {self.difficulty}")
        return self.difficulty


if __name__ == '__main__':
    print("--- PoW Consensus Basic Test ---")

    try:
        # Mock a last block (like a genesis block)
        mock_genesis_txs = [{"tx_id": "genesis", "data": "mock genesis"}]
        mock_last_block = Block(0, mock_genesis_txs, time.time() - 10, "0", 0, "system")

        difficulty = 2  # Start with lower difficulty for testing (2 leading zeros)
        pow_consensus = PoWConsensus(difficulty=difficulty)

        # Transactions for the new block
        try:
            new_transactions = [
                Transaction(transaction_type="test_tx", asset_id_value="asset123", agent_ids={"creator_agent_id":"test_user"}).to_dict()
            ]
        except Exception as e:
            print(f"Transaction creation failed: {e}")
            print("Using simple dictionary instead...")
            new_transactions = [
                {"transaction_type": "test_tx", "asset_id_value": "asset123", "agent_ids": {"creator_agent_id": "test_user"}}
            ]

        print(f"Mining with transactions: {new_transactions}")

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
                sealer_id="test_miner",
            )
            
            # Verify the hash matches
            if newly_mined_block.hash == found_hash:
                print("✅ Block hash matches mined hash!")
            else:
                print(f"❌ Hash mismatch! Mined: {found_hash}, Block: {newly_mined_block.hash}")

            print(f"\nNewly Mined Block: {newly_mined_block}")

            # Validate the proof
            is_valid = pow_consensus.validate_proof(newly_mined_block)
            print(f"Validation of mined block's proof: {is_valid}")
            
            if not is_valid:
                print("❌ Mined block failed validation!")
            else:
                print("✅ Mined block passed validation!")

            # Test validation with a block that shouldn't pass
            print("\n--- Testing Invalid Block ---")
            invalid_block = Block(
                index=mock_last_block.index + 1,
                transactions=new_transactions,
                timestamp=time.time(),
                previous_hash=mock_last_block.hash,
                nonce=123,  # Arbitrary nonce, likely won't meet difficulty
                sealer_id="test_miner"
            )
            print(f"Invalid Block (for testing validation): {invalid_block}")
            is_invalid_proof_valid = pow_consensus.validate_proof(invalid_block)
            print(f"Validation of invalid block's proof: {is_invalid_proof_valid}")
            
            if is_invalid_proof_valid:
                print("❌ Invalid block unexpectedly passed validation!")
            else:
                print("✅ Invalid block correctly failed validation!")

            # Optional: Test with higher difficulty (uncomment to test)
            print("\n--- Testing with higher difficulty ---")
            print("Testing with difficulty 3 (may take longer)...")
            pow_consensus_harder = PoWConsensus(difficulty=3)
            mined_info_harder = pow_consensus_harder.proof_of_work(newly_mined_block, [{"data": "another tx"}])
            if mined_info_harder:
                print(f"✅ Mined with higher difficulty: Nonce {mined_info_harder[0]}, Hash {mined_info_harder[1][:10]}...")
            else:
                print("⏱️ Mining with higher difficulty timed out.")

        else:
            print("❌ Mining failed - this should not happen unless timeout occurred.")

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

    print("\n✅ PoW Consensus tests finished.")