import time
import hashlib
import json

class Block:
    """
    Represents a block in the blockchain.

    Attributes:
        index (int): The position of the block in the chain.
        transactions (list): A list of transactions included in this block.
                             Expected to be serializable (e.g., list of dictionaries).
        timestamp (float): Time of block creation (Unix timestamp).
        previous_hash (str): Hash of the preceding block in the chain.
        nonce (int): A number used in some consensus algorithms (e.g., PoW for mining,
                     or other purposes like sequence number in PBFT).
        sealer_id (str, optional): Identifier of the agent/authority that sealed/created this block.
                                   Used in PoA, PBFT, etc. Defaults to None.
        hash (str): The calculated SHA256 hash of the block's content.
    """
    def __init__(self, index: int, transactions: list, timestamp: float,
                 previous_hash: str, nonce: int = 0, sealer_id: str = None):
        """
        Constructs a new Block.

        Args:
            index (int): Index of the block.
            transactions (list): List of transactions (should be serializable).
            timestamp (float): Timestamp of block creation.
            previous_hash (str): Hash of the previous block.
            nonce (int, optional): Nonce value. Defaults to 0.
            sealer_id (str, optional): ID of the block sealer/creator. Defaults to None.
        """
        self.index = index
        self.transactions = transactions
        self.timestamp = timestamp
        self.previous_hash = previous_hash
        self.nonce = nonce
        self.sealer_id = sealer_id
        self.qc = None # For HotStuff: Quorum Certificate for the parent block.
        self.hash = self.calculate_hash()

    def calculate_hash(self) -> str:
        """
        Calculates the SHA256 hash of the block's content.

        The block's content (index, transactions, timestamp, previous_hash, nonce, sealer_id, qc)
        is serialized into a JSON string (with sorted keys for consistency)
        before hashing.

        Returns:
            str: The hexadecimal string representation of the hash.
        """
        # QC needs to be serializable if it's an object.
        # For now, let's assume it can be converted to a dict or is None.
        qc_data = self.qc.to_dict() if hasattr(self.qc, 'to_dict') else self.qc

        block_dict = {
            'index': self.index,
            'transactions': self.transactions,
            'timestamp': self.timestamp,
            'previous_hash': self.previous_hash,
            'nonce': self.nonce,
            'sealer_id': self.sealer_id,
            'qc': qc_data
        }
        # Ensure the dictionary is sorted by keys for consistent hash results
        block_string = json.dumps(block_dict, sort_keys=True).encode('utf-8')
        return hashlib.sha256(block_string).hexdigest()

    def __repr__(self) -> str:
        """
        Returns a string representation of the Block.
        """
        return (f"Block(Index: {self.index}, Hash: {self.hash[:8]}..., Prev_Hash: {self.previous_hash[:8] if self.previous_hash != '0' else '0'}..., "
                f"Nonce: {self.nonce}, Sealer: {self.sealer_id}, Transactions: {len(self.transactions)})")

if __name__ == '__main__':
    # Example Usage
    genesis_block_txs = [{"tx_id": "genesis_tx", "data": "Genesis Block Transaction"}]
    genesis_block = Block(
        index=0,
        transactions=genesis_block_txs,
        timestamp=time.time(),
        previous_hash="0",
        sealer_id="system_genesis"
    )
    print(genesis_block)

    block1_txs = [
        {"tx_id": "tx001", "data": "Transaction A to B"},
        {"tx_id": "tx002", "data": "Transaction C to D"}
    ]
    second_block = Block(
        index=1,
        transactions=block1_txs,
        timestamp=time.time(),
        previous_hash=genesis_block.hash,
        nonce=123,
        sealer_id="authority_Alpha"
    )
    print(second_block)

    # Verify hash calculation consistency
    assert genesis_block.hash == genesis_block.calculate_hash()
    assert second_block.hash == second_block.calculate_hash()
    print("\nBlock class basic tests passed.")
