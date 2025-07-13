from dataclasses import dataclass, asdict
from typing import List, Any
from src.blockchain.block import Block # For type hinting

@dataclass
class QuorumCertificate:
    """
    Represents a Quorum Certificate (QC) in the HotStuff protocol.
    A QC acts as proof that a quorum of replicas (2f+1) has voted for a block.

    Attributes:
        view_number (int): The view in which the block was proposed.
        block_hash (str): The hash of the block that this QC certifies.
        signatures (List[str]): A list of node IDs that contributed to the quorum.
                                In a real implementation, these would be cryptographic signatures.
    """
    view_number: int
    block_hash: str
    signatures: List[str]

    def to_dict(self) -> dict:
        """Serializes the QC object to a dictionary."""
        return asdict(self)

@dataclass
class ProposalMessage:
    """
    Represents a PROPOSAL message sent by the leader to replicas.
    It contains the newly proposed block. The block itself contains the QC
    for its parent, linking the chain of certifications.

    Attributes:
        block (Block): The proposed Block object.
        sender_id (str): The ID of the leader sending the proposal.
    """
    block: Block
    sender_id: str

@dataclass
class VoteMessage:
    """
    Represents a VOTE message sent by a replica to the next leader.
    This message signifies a vote for the block referenced by `block_hash`.

    Attributes:
        block_hash (str): The hash of the block being voted on.
        signature_share (str): The node ID of the replica casting the vote.
                               In a real system, this would be a partial signature.
    """
    block_hash: str
    signature_share: str # This is just the node_id for simulation


if __name__ == '__main__':
    # Example Usage

    # Mock signatures from nodes N1, N2, N3 for a quorum of 3 (if N=4, f=1, 2f+1=3)
    mock_signatures = ["N1", "N2", "N3"]
    qc = QuorumCertificate(view_number=1, block_hash="hash_of_parent_block", signatures=mock_signatures)
    print(f"Example QC: {qc}")
    print(f"QC as dict: {qc.to_dict()}")

    # Mock a block that contains this QC (certifying its parent)
    # The block's own hash would be calculated based on its content, including the QC.
    mock_block = Block(
        index=2,
        transactions=[{"tx_id": "tx1"}],
        timestamp=123456.789,
        previous_hash="hash_of_parent_block",
        sealer_id="leader_of_view_2"
    )
    mock_block.qc = qc # Attach the QC for the parent

    proposal = ProposalMessage(block=mock_block, sender_id="leader_of_view_2")
    print(f"\nExample ProposalMessage: {proposal}")

    vote = VoteMessage(block_hash=mock_block.hash, signature_share="N1")
    print(f"\nExample VoteMessage: {vote}")
