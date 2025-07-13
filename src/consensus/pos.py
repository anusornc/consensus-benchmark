import hashlib
import time
from typing import Dict, List
from src.blockchain.block import Block

class PoSConsensus:
    """
    Implements a deterministic Proof of Stake (PoS) consensus mechanism.

    In this simplified PoS model, validators are chosen to create the next block
    based on their stake. The selection is deterministic, derived from the hash
    of the last block, providing a weighted, pseudo-random but repeatable outcome
    for a given chain state.

    Attributes:
        validators (Dict[str, int]): A dictionary mapping validator IDs to their stake amount.
        weighted_validator_list (List[str]): A list where each validator appears a number
                                             of times equal to their stake, used for selection.
        total_stake (int): The sum of all stakes from all validators.
    """
    def __init__(self, validators: Dict[str, int]):
        """
        Initializes the PoS consensus mechanism.

        Args:
            validators (Dict[str, int]): A dictionary of validator IDs and their stakes.
                                         Example: {"validator1": 50, "validator2": 30}

        Raises:
            ValueError: If the validators dictionary is empty.
        """
        if not validators:
            raise ValueError("Validators dictionary cannot be empty for PoS consensus.")

        self.validators = validators
        self.weighted_validator_list: List[str] = []
        self.total_stake = 0

        for validator, stake in self.validators.items():
            if stake > 0:
                self.weighted_validator_list.extend([validator] * stake)
                self.total_stake += stake

        if self.total_stake == 0:
            raise ValueError("Total stake must be greater than zero.")

        print(f"PoSConsensus initialized with {len(self.validators)} validators and total stake of {self.total_stake}.")

    def select_next_validator(self, last_block_hash: str) -> str:
        """
        Deterministically selects the next block's validator based on the last block's hash.

        Args:
            last_block_hash (str): The hash of the previous block.

        Returns:
            str: The ID of the selected validator.
        """
        # Use the last block hash to generate a number for selection
        # We take the integer representation of the first 8 hex characters
        hash_int = int(last_block_hash[:8], 16)
        selection_index = hash_int % self.total_stake

        return self.weighted_validator_list[selection_index]

    def create_block(self, last_block: Block, transactions: list[dict]) -> Block:
        """
        Creates a new block, sealed by the validator chosen by the PoS mechanism.

        Args:
            last_block (Block): The last block in the blockchain.
            transactions (list[dict]): A list of transactions to include in the new block.

        Returns:
            Block: The newly created Block object.
        """
        validator = self.select_next_validator(last_block.hash)
        print(f"PoS: Validator '{validator}' selected to create block {last_block.index + 1}.")

        new_block = Block(
            index=last_block.index + 1,
            transactions=transactions,
            timestamp=time.time(),
            previous_hash=last_block.hash,
            nonce=0,  # Nonce is not used for mining in PoS
            sealer_id=validator
        )
        return new_block

    def validate_block(self, block: Block, last_block: Block) -> bool:
        """
        Validates a block according to PoS rules.

        Checks if the block's sealer_id matches the deterministically chosen validator
        based on the hash of the last block.

        Args:
            block (Block): The block to validate.
            last_block (Block): The block preceding the one being validated.

        Returns:
            bool: True if the block's sealer is the correct one, False otherwise.
        """
        expected_validator = self.select_next_validator(last_block.hash)
        if block.sealer_id != expected_validator:
            print(f"PoS Validate: Invalid sealer for block {block.index}. "
                  f"Expected '{expected_validator}', but got '{block.sealer_id}'.")
            return False

        # This PoS implementation doesn't have other complex rules to check here.
        # General block validity (hash, index) is handled by Blockchain class.
        return True


if __name__ == '__main__':
    print("--- PoS Consensus Basic Test ---")

    validators = {"validator-A": 10, "validator-B": 20, "validator-C": 70}
    pos_consensus = PoSConsensus(validators)

    # Mock a genesis block
    mock_genesis_block = Block(0, [], time.time() - 20, "0", 0, "system_genesis")
    print(f"Genesis block hash: {mock_genesis_block.hash}")

    # --- Round 1 ---
    # Select validator for block 1
    validator_for_block1 = pos_consensus.select_next_validator(mock_genesis_block.hash)
    print(f"Validator selected for block 1: {validator_for_block1}")

    # Create block 1
    block1 = pos_consensus.create_block(mock_genesis_block, [{"tx": "data1"}])
    assert block1.sealer_id == validator_for_block1
    print(f"Created Block 1: {block1}")

    # Validate block 1
    is_valid1 = pos_consensus.validate_block(block1, mock_genesis_block)
    assert is_valid1, "Block 1 should be valid"
    print(f"Validation of Block 1: {is_valid1}")

    # --- Round 2 ---
    # Select validator for block 2
    validator_for_block2 = pos_consensus.select_next_validator(block1.hash)
    print(f"\nValidator selected for block 2: {validator_for_block2}")

    # Create block 2
    block2 = pos_consensus.create_block(block1, [{"tx": "data2"}])
    assert block2.sealer_id == validator_for_block2
    print(f"Created Block 2: {block2}")

    # Validate block 2
    is_valid2 = pos_consensus.validate_block(block2, block1)
    assert is_valid2, "Block 2 should be valid"
    print(f"Validation of Block 2: {is_valid2}")

    # Test invalid sealer
    block2_tampered = Block(
        index=block2.index, transactions=block2.transactions, timestamp=block2.timestamp,
        previous_hash=block2.previous_hash, nonce=block2.nonce,
        sealer_id="not_the_chosen_one"
    )
    is_valid_tampered = pos_consensus.validate_block(block2_tampered, block1)
    assert not is_valid_tampered, "Tampered block should be invalid"
    print(f"\nValidation of tampered Block 2: {is_valid_tampered}")

    print("\nPoS Consensus basic tests finished.")
