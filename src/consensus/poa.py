import time
from src.blockchain.block import Block
from src.transactions.transaction import Transaction # For type hinting or direct use if needed

class PoAConsensus:
    def __init__(self, authorities: list[str]):
        """
        Initializes the Proof of Authority consensus mechanism.

        Args:
            authorities (list[str]): A list of identifiers for the approved authorities.
        """
        if not authorities:
            raise ValueError("Authorities list cannot be empty for PoA consensus.")
        self.authorities = set(authorities)
        # self.last_block_sealer is removed; sealer info is now read from the block itself.

    def validate_authority(self, authority_address: str) -> bool:
        """
        Checks if the given address belongs to a recognized authority.
        """
        return authority_address in self.authorities

    def can_seal_block(self, authority_address: str, blockchain_instance) -> bool:
        """
        Checks if the given authority is allowed to seal the next block.
        - Must be a valid authority.
        - Cannot be the same as the sealer of the immediate last block (if chain has blocks).

        Args:
            authority_address (str): The address of the authority attempting to seal.
            blockchain_instance (Blockchain): The current blockchain instance.
        Returns:
            bool: True if the authority can seal, False otherwise.
        """
        if not self.validate_authority(authority_address):
            print(f"Authority {authority_address} is not a recognized authority.")
            return False

        # Genesis block creation might be handled specially or have no prior sealer.
        # If chain is empty or only has genesis, any authority can seal.
        if len(blockchain_instance.chain) == 0: # Should not happen if genesis is always there
             return True
        if len(blockchain_instance.chain) == 1 and blockchain_instance.chain[0].previous_hash == "0": # Only genesis block exists
            return True


        last_block = blockchain_instance.last_block
        if last_block.sealer_id is None:
            # This case implies the last block wasn't sealed under PoA rules or sealer_id is missing.
            # Depending on strictness, this could be an error or allow sealing.
            # For now, let's assume if sealer_id is None, it doesn't restrict the next sealer.
            # However, a PoA chain should ideally always have sealer_id for non-genesis blocks.
            print(f"Warning: Last block (Index: {last_block.index}) has no sealer_id. Allowing {authority_address} to seal.")
            return True

        if last_block.sealer_id == authority_address:
             print(f"Authority {authority_address} sealed the last block (Block {last_block.index} by {last_block.sealer_id}) and cannot seal consecutively.")
             return False

        return True

    def create_block(self, blockchain_instance, authority_address: str, transactions: list[dict], nonce: int = 0):
        """
        Creates a new block if the authority is valid and permitted to seal.
        The new block will have `sealer_id` set to `authority_address`.

        Args:
            blockchain_instance (Blockchain): The blockchain to add the block to.
            authority_address (str): The identifier of the authority creating the block.
            transactions (list[dict]): A list of transactions (as dictionaries) to include in the block.
            nonce (int, optional): A nonce for the block. Defaults to 0.

        Returns:
            Block: The newly created Block object, or None if creation failed.
        """
        if not self.can_seal_block(authority_address, blockchain_instance):
            # can_seal_block already prints the reason
            return None

        previous_hash = blockchain_instance.last_block.hash
        index = len(blockchain_instance.chain)

        block = Block(
            index=index,
            transactions=transactions,
            timestamp=time.time(),
            previous_hash=previous_hash,
            nonce=nonce,
            sealer_id=authority_address # Set the sealer_id on the block
        )

        return block

    def validate_block(self, block_to_validate: Block, previous_block: Block = None) -> bool:
        """
        Validates a block according to PoA rules.
        - Block's sealer_id must be a recognized authority.
        - Block's sealer_id must not be the same as the previous_block's sealer_id (if previous_block exists and has a sealer).

        Args:
            block_to_validate (Block): The block to validate.
            previous_block (Block, optional): The previous block in the chain. Required for the consecutive sealer check.
                                              If None, only authority validity is checked (e.g. for first block after genesis).

        Returns:
            bool: True if the block is valid under PoA rules, False otherwise.
        """
        if not block_to_validate.sealer_id:
            print(f"Block {block_to_validate.index} has no sealer_id. Invalid for PoA.")
            return False

        if not self.validate_authority(block_to_validate.sealer_id):
            print(f"Block {block_to_validate.index} sealed by an unrecognized authority: {block_to_validate.sealer_id}.")
            return False

        # Check for consecutive sealing rule if previous block is provided
        if previous_block:
            if previous_block.sealer_id is None:
                # If previous block has no sealer_id, this rule might not apply or it's a chain integrity issue.
                # For this basic check, we'll assume it's fine if prev sealer is unknown.
                # A stricter PoA might require all blocks to have sealers.
                pass # Allow, but could log a warning.
            elif previous_block.sealer_id == block_to_validate.sealer_id:
                print(f"Block {block_to_validate.index} (Sealer: {block_to_validate.sealer_id}) cannot be sealed by the same authority as previous block (Sealer: {previous_block.sealer_id}).")
                return False

        # Note: This doesn't re-validate block.hash or block.previous_hash.
        # That's typically done by Blockchain.is_chain_valid().
        # This focuses purely on PoA specific rules.
        return True

if __name__ == '__main__':
    # Mock Blockchain class for testing PoA
    class MockBlockchain:
        def __init__(self, genesis_sealer="genesis_authority_id"):
            self.chain = []
            # Create a mock genesis block
            genesis_tx = Transaction("system", "genesis", {"message": "Genesis Block"}).to_dict()
            # Genesis block might have a special sealer or None, depending on PoA chain rules.
            # Let's assume it has a defined sealer for consistency in tests.
            self.genesis_block = Block(0, [genesis_tx], time.time(), "0", sealer_id=genesis_sealer)
            self.chain.append(self.genesis_block)

        @property
        def last_block(self):
            return self.chain[-1]

        def add_block(self, block):
            self.chain.append(block)

    # --- Test PoAConsensus ---
    authorities_list = ["authority1", "authority2", "authority3"]
    poa_consensus = PoAConsensus(authorities_list)

    mock_bc = MockBlockchain()
    poa_consensus.last_block_sealer = mock_bc.last_block.hash # Initialize with genesis sealer if needed (or None)

    print("--- PoA Consensus Test ---")
    print(f"Authorities: {poa_consensus.authorities}")

    # Authority 1 creates the first block (after genesis)
    tx_list1 = [Transaction("Alice", "Bob", {"data": "tx1"}).to_dict()]
    print(f"\nAuthority1 attempting to create block 1...")
    block1 = poa_consensus.create_block(mock_bc, "authority1", tx_list1, nonce=1)
    if block1:
        mock_bc.add_block(block1)
        print(f"Block 1 created by authority1: {block1}")
    else:
        print(f"Block 1 creation failed.")
    print(f"Chain length: {len(mock_bc.chain)}")
    print(f"Last sealer: {poa_consensus.last_block_sealer}")


    # Authority 1 tries to create the next block (should fail)
    tx_list2 = [Transaction("Charlie", "David", {"data": "tx2"}).to_dict()]
    print(f"\nAuthority1 attempting to create block 2 (consecutively)...")
    block2_by_auth1 = poa_consensus.create_block(mock_bc, "authority1", tx_list2, nonce=2)
    if block2_by_auth1:
        # mock_bc.add_block(block2_by_auth1) # Should not happen
        print(f"ERROR: Block 2 created by authority1 consecutively: {block2_by_auth1}")
    else:
        print(f"Block 2 creation by authority1 failed (as expected).")
    print(f"Chain length: {len(mock_bc.chain)}")
    print(f"Last sealer: {poa_consensus.last_block_sealer}")

    # Authority 2 creates the next block (should succeed)
    print(f"\nAuthority2 attempting to create block 2...")
    block2_by_auth2 = poa_consensus.create_block(mock_bc, "authority2", tx_list2, nonce=2)
    if block2_by_auth2:
        mock_bc.add_block(block2_by_auth2)
        print(f"Block 2 created by authority2: {block2_by_auth2}")
    else:
        print(f"Block 2 creation by authority2 failed.")
    print(f"Chain length: {len(mock_bc.chain)}")
    print(f"Last sealer: {poa_consensus.last_block_sealer}")

    # Non-authority tries to create a block
    tx_list3 = [Transaction("Eve", "Mallory", {"data": "tx3"}).to_dict()]
    print(f"\nNonAuthority attempting to create block 3...")
    block3_by_hacker = poa_consensus.create_block(mock_bc, "non_authority_hacker", tx_list3, nonce=3)
    if block3_by_hacker:
        # mock_bc.add_block(block3_by_hacker) # Should not happen
        print(f"ERROR: Block 3 created by non_authority_hacker: {block3_by_hacker}")
    else:
        print(f"Block 3 creation by non_authority_hacker failed (as expected).")
    print(f"Chain length: {len(mock_bc.chain)}")
    print(f"Last sealer: {poa_consensus.last_block_sealer}")

    # Test validation (basic placeholder for now)
    if block1:
        print(f"\nValidating block1: {poa_consensus.validate_block(block1)}")
    if block2_by_auth2:
        # To test the "cannot seal consecutively" rule in validate_block, we'd need to pass the previous sealer.
        # This requires sealer info in the block itself.
        # poa_consensus.validate_block(block2_by_auth2, previous_block_sealer="authority1")
        print(f"Validating block2_by_auth2: {poa_consensus.validate_block(block2_by_auth2)}")

    # Test with empty authorities list
    try:
        PoAConsensus([])
    except ValueError as e:
        print(f"\nCaught expected error for empty authorities: {e}")

    print("\nPoA Consensus Test Finished.")
