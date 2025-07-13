import time
from typing import List
from src.blockchain.block import Block

class DPoSConsensus:
    """
    Implements a simplified Delegated Proof of Stake (DPoS) consensus mechanism.

    In DPoS, a fixed set of elected "delegates" take turns producing blocks in a
    round-robin schedule. This implementation assumes the delegate set is pre-defined.

    Attributes:
        delegates (List[str]): An ordered list of unique identifiers for the delegates.
        num_delegates (int): The total number of delegates.
    """
    def __init__(self, delegates: List[str]):
        """
        Initializes the DPoS consensus mechanism.

        Args:
            delegates (List[str]): An ordered list of delegate IDs. This order
                                   determines the block production schedule.

        Raises:
            ValueError: If the delegates list is empty.
        """
        if not delegates:
            raise ValueError("Delegates list cannot be empty for DPoS consensus.")

        self.delegates = delegates
        self.num_delegates = len(self.delegates)
        print(f"DPoSConsensus initialized with {self.num_delegates} delegates: {self.delegates}")

    def get_current_delegate(self, block_index: int) -> str:
        """
        Determines which delegate's turn it is to produce a block.

        Args:
            block_index (int): The index of the block to be produced.

        Returns:
            str: The ID of the delegate whose turn it is.
        """
        delegate_index = block_index % self.num_delegates
        return self.delegates[delegate_index]

    def create_block(self, last_block: Block, transactions: list[dict]) -> Block:
        """
        Creates a new block, sealed by the delegate whose turn it is.

        Args:
            last_block (Block): The last block in the blockchain.
            transactions (list[dict]): A list of transactions to include.

        Returns:
            Block: The newly created Block object.
        """
        new_block_index = last_block.index + 1
        delegate = self.get_current_delegate(new_block_index)
        print(f"DPoS: Delegate '{delegate}' is scheduled to create block {new_block_index}.")

        new_block = Block(
            index=new_block_index,
            transactions=transactions,
            timestamp=time.time(),
            previous_hash=last_block.hash,
            nonce=0,  # Nonce is not used for mining in DPoS
            sealer_id=delegate
        )
        return new_block

    def validate_block(self, block: Block) -> bool:
        """
        Validates a block according to DPoS rules.

        Checks if the block's sealer_id matches the delegate scheduled for that
        block index.

        Args:
            block (Block): The block to validate.

        Returns:
            bool: True if the block's sealer is the correct delegate, False otherwise.
        """
        expected_delegate = self.get_current_delegate(block.index)
        if block.sealer_id != expected_delegate:
            print(f"DPoS Validate: Invalid sealer for block {block.index}. "
                  f"Expected '{expected_delegate}', but got '{block.sealer_id}'.")
            return False

        return True


if __name__ == '__main__':
    print("--- DPoS Consensus Basic Test ---")

    delegates = ["delegate-A", "delegate-B", "delegate-C"]
    dpos_consensus = DPoSConsensus(delegates)

    # Mock a genesis block
    mock_genesis_block = Block(0, [], time.time() - 20, "0", 0, "system_genesis")
    print(f"Genesis block: {mock_genesis_block}")

    # --- Round 1: Block 1 ---
    delegate_for_block1 = dpos_consensus.get_current_delegate(1)
    assert delegate_for_block1 == "delegate-B" # index 1 % 3 = 1
    block1 = dpos_consensus.create_block(mock_genesis_block, [{"tx": "data1"}])
    assert block1.sealer_id == delegate_for_block1
    print(f"Created Block 1: {block1}")
    is_valid1 = dpos_consensus.validate_block(block1)
    assert is_valid1, "Block 1 should be valid"
    print(f"Validation of Block 1: {is_valid1}")

    # --- Round 2: Block 2 ---
    delegate_for_block2 = dpos_consensus.get_current_delegate(2)
    assert delegate_for_block2 == "delegate-C" # index 2 % 3 = 2
    block2 = dpos_consensus.create_block(block1, [{"tx": "data2"}])
    assert block2.sealer_id == delegate_for_block2
    print(f"\nCreated Block 2: {block2}")
    is_valid2 = dpos_consensus.validate_block(block2)
    assert is_valid2, "Block 2 should be valid"
    print(f"Validation of Block 2: {is_valid2}")

    # --- Round 3: Block 3 (cycle back) ---
    delegate_for_block3 = dpos_consensus.get_current_delegate(3)
    assert delegate_for_block3 == "delegate-A" # index 3 % 3 = 0
    block3 = dpos_consensus.create_block(block2, [{"tx": "data3"}])
    assert block3.sealer_id == delegate_for_block3
    print(f"\nCreated Block 3: {block3}")
    is_valid3 = dpos_consensus.validate_block(block3)
    assert is_valid3, "Block 3 should be valid"
    print(f"Validation of Block 3: {is_valid3}")

    # Test invalid sealer
    block3_tampered = Block(
        index=block3.index, transactions=block3.transactions, timestamp=block3.timestamp,
        previous_hash=block3.previous_hash, nonce=block3.nonce,
        sealer_id="not_a_delegate"
    )
    is_valid_tampered = dpos_consensus.validate_block(block3_tampered)
    assert not is_valid_tampered, "Tampered block should be invalid"
    print(f"\nValidation of tampered Block 3: {is_valid_tampered}")

    print("\nDPoS Consensus basic tests finished.")
