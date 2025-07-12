import time
from collections import defaultdict
from typing import Dict, List, Optional

# Assuming these classes are in the same directory or project structure is handled.
from src.blockchain.blockchain import Blockchain
from src.blockchain.block import Block
from .hotstuff_messages import QuorumCertificate, ProposalMessage, VoteMessage

class HotStuffNode:
    """
    Represents a single node participating in the HotStuff consensus protocol.
    (This is an initial, simplified implementation).

    Attributes:
        node_id (str): Unique identifier for this node.
        all_node_ids (List[str]): List of all node IDs in the network.
        blockchain (Blockchain): The node's local instance of the blockchain.
        n (int): Total number of nodes.
        f (int): Maximum number of faulty nodes the system can tolerate (n = 3f + 1).

        current_view (int): The current view number this node is in.
        prepare_qc (QuorumCertificate | None): The highest QC the node has seen.
        locked_qc (QuorumCertificate | None): The QC for the last locked block.

        # For the leader:
        votes_for_current_proposal (Dict[str, VoteMessage]): Stores votes for the current proposal.

        # For replicas:
        voted_view (int): The last view number this node has cast a vote in.

        network_simulator (any): Reference to the network simulation environment.
        primary_id (str): The ID of the current leader/primary for the current view.
        is_primary_node (bool): True if this node is the current leader.
    """
    def __init__(self, node_id: str, all_node_ids: list[str], blockchain: Blockchain):
        self.node_id = node_id
        self.all_node_ids = sorted(list(set(all_node_ids)))
        self.blockchain = blockchain

        self.n = len(self.all_node_ids)
        self.f = (self.n - 1) // 3
        self.quorum_size = 2 * self.f + 1

        self.current_view = 0
        self.prepare_qc: Optional[QuorumCertificate] = None # Highest QC node has seen
        self.locked_qc: Optional[QuorumCertificate] = None  # Highest QC that is part of a 2-chain

        self.votes_for_current_proposal: Dict[str, VoteMessage] = {}
        self.voted_view = -1 # Tracks the view of the last vote cast by this replica
        self.block_store: Dict[str, Block] = {} # Store blocks by hash for easy lookup

        self.pending_client_requests = []
        self.network_simulator = None
        self.update_primary()
        print(f"HotStuffNode {self.node_id} initialized. N={self.n}, f={self.f}, QuorumSize={self.quorum_size}. Primary for view 0: {self.primary_id}")

    def set_network_simulator(self, simulator):
        self.network_simulator = simulator

    def update_primary(self):
        """Determines the primary (leader) for the current view."""
        self.primary_id = self.all_node_ids[self.current_view % self.n]
        self.is_primary_node = (self.node_id == self.primary_id)

    def is_primary(self) -> bool:
        return self.is_primary_node

    def broadcast_message(self, message):
        if self.network_simulator:
            self.network_simulator.broadcast(self.node_id, message)
        else:
            print(f"HotStuffNode {self.node_id} [No Network]: Broadcasting {message.__class__.__name__}")

    # --- Client Request and Proposal (Leader Logic) ---

    def handle_client_request(self, request_payload: any):
        """Handles a new client request payload (for the leader)."""
        if not self.is_primary():
            # In a real system, would forward to leader. Here, we just ignore.
            print(f"HotStuffNode {self.node_id}: Not primary, ignoring client request.")
            return

        self.pending_client_requests.append(request_payload)
        # Leader might propose immediately or batch. `on_beat` will handle proposal.
        print(f"HotStuffNode {self.node_id} (Leader): Queued client request. Total pending: {len(self.pending_client_requests)}")

    def on_beat(self):
        """
        Pacemaker-driven method for the leader to create and broadcast a proposal.
        This simulates the leader taking action at the start of a view or round.
        """
        if not self.is_primary() or not self.pending_client_requests:
            return

        # Create a new block proposal
        new_block = self.create_proposal()

        # Broadcast the proposal
        proposal_msg = ProposalMessage(block=new_block, sender_id=self.node_id)
        print(f"HotStuffNode {self.node_id} (Leader, V:{self.current_view}): Proposing Block {new_block.index} with hash {new_block.hash[:8]}... and parent QC {new_block.qc.block_hash[:8] if new_block.qc else 'None'}")
        self.broadcast_message(proposal_msg)


    def create_proposal(self) -> Block:
        """Leader logic to create a new block proposal."""
        last_block = self.blockchain.last_block
        if not last_block:
            # This should only happen for the very first block (genesis)
            # HotStuff genesis procedure is special. For now, assume a pre-existing genesis.
            raise ValueError("HotStuffNode cannot create proposal without a genesis block.")

        # For a new proposal, we extend from the block referenced by the highest QC we know (prepare_qc)
        # This is a key part of HotStuff's chain-of-QCs structure.
        parent_block_hash = self.prepare_qc.block_hash if self.prepare_qc else last_block.hash

        # This lookup is a simplification. A real node would have a block store keyed by hash.
        # Here we assume the last block in the local chain is the one we need for the index.
        # This will break if the chain has forks, which HotStuff can create temporarily.
        parent_block_for_index = self.blockchain.last_block

        new_block = Block(
            index=parent_block_for_index.index + 1,
            transactions=list(self.pending_client_requests),
            timestamp=time.time(),
            previous_hash=parent_block_hash,
            sealer_id=self.node_id,
            nonce=self.current_view # Use nonce to store the view number
        )
        new_block.qc = self.prepare_qc # Embed the highest QC into the new block
        new_block.hash = new_block.calculate_hash() # Recalculate hash with QC

        # Clear pending requests that were included
        self.pending_client_requests.clear()
        return new_block


    # --- Replica Logic ---

    def on_receive_proposal(self, proposal_msg: ProposalMessage):
        """Handles a new block proposal from the leader."""
        block = proposal_msg.block
        print(f"HotStuffNode {self.node_id} (V:{self.current_view}): Received proposal for Block {block.index} from {proposal_msg.sender_id}")

        # Add the block to our local store for lookup
        self.block_store[block.hash] = block
        # Also add to the blockchain instance to maintain a linear "committed" chain (for now)
        # This part is tricky. In real HotStuff, you maintain a tree and only add to the final chain upon commit.
        # For simulation, we'll add it but only "execute" later.
        if not any(b.hash == block.hash for b in self.blockchain.chain):
             self.blockchain.chain.append(block) # Simplification

        # Basic validation
        if block.nonce < self.current_view: # Check if block's view is current
            print(f"HotStuffNode {self.node_id}: Ignoring proposal from past view {block.nonce}")
            return

        # Update our highest known QC if the one in the proposal is newer
        if block.qc and (self.prepare_qc is None or block.qc.view_number > self.prepare_qc.view_number):
            self.prepare_qc = block.qc

        # --- HotStuff Safety Rule ---
        # A replica only votes if the proposal extends from its locked_qc.
        is_safe_to_vote = False
        if self.locked_qc is None:
            is_safe_to_vote = True
        elif block.previous_hash == self.locked_qc.block_hash:
            is_safe_to_vote = True

        if is_safe_to_vote:
            print(f"HotStuffNode {self.node_id}: Proposal is safe. Voting for block {block.hash[:8]}...")
            vote_msg = VoteMessage(block_hash=block.hash, signature_share=self.node_id)
            if self.network_simulator:
                self.network_simulator.send(self.node_id, self.primary_id, vote_msg)
        else:
            print(f"HotStuffNode {self.node_id}: Not voting for proposal. It does not extend locked_qc ({self.locked_qc.block_hash[:8] if self.locked_qc else 'None'}).")

        # --- HotStuff Liveness Rule & State Update ---
        if block.qc and (self.prepare_qc is None or block.qc.view_number > self.prepare_qc.view_number):
            self.prepare_qc = block.qc
            # The logic to update locked_qc and commit will now be handled in on_receive_vote by the leader,
            # and then propagated in subsequent proposals.
            pass


    def on_receive_vote(self, vote_msg: VoteMessage):
        """Handles a vote message (for the leader)."""
        if not self.is_primary():
            return # Ignore votes if not the current leader

        print(f"HotStuffNode {self.node_id} (Leader): Received vote for block {vote_msg.block_hash[:8]} from {vote_msg.signature_share}")

        # Store the vote
        self.votes_for_current_proposal[vote_msg.signature_share] = vote_msg

        # Check if a quorum has been reached
        # This is a simplified check; it should check that all votes are for the *same* block hash.
        if len(self.votes_for_current_proposal) >= self.quorum_size:
            print(f"HotStuffNode {self.node_id} (Leader): Quorum of {self.quorum_size} reached for block {vote_msg.block_hash[:8]}!")

            # Create a new Quorum Certificate
            new_qc = QuorumCertificate(
                view_number=self.current_view,
                block_hash=vote_msg.block_hash,
                signatures=list(self.votes_for_current_proposal.keys())
            )

            # Clear votes for the next proposal
            self.votes_for_current_proposal.clear()

            # --- Simplified State Update & Commit Logic (now inside the quorum check) ---
            previous_prepare_qc = self.prepare_qc
            self.prepare_qc = new_qc
            print(f"HotStuffNode {self.node_id} (Leader): Created new Prepare QC for block {new_qc.block_hash[:8]} in view {new_qc.view_number}")

            # Check for a 2-chain to update the locked_qc
            # A 2-chain is formed if the new QC's block is a child of the block certified by the previous QC.
            # This requires looking up the block, which is complex.
            # Simplified check: if the views are consecutive. This is a weak proxy for parent-child relationship.
            if previous_prepare_qc and new_qc.view_number == previous_prepare_qc.view_number + 1:
                self.locked_qc = previous_prepare_qc
                print(f"HotStuffNode {self.node_id} (Leader): Updated locked_qc to QC for view {self.locked_qc.view_number}")

                # Check for a 3-chain to commit a block
                # Simplified commit rule: When we lock a QC (previous_prepare_qc), we commit its parent.
                # The block to be committed is the one certified by the *newly promoted* locked_qc's parent.
                # This is still very simplified. A real implementation would check block.qc.block_hash.
                if self.locked_qc.view_number > 0: # Cannot commit genesis
                    # This logic is flawed as it relies on local chain indexing, not block hashes.
                    # This is a placeholder for a proper commit rule.
                    try:
                        parent_of_locked_block_qc = self.blockchain.chain[self.locked_qc.view_number].qc
                        if parent_of_locked_block_qc:
                            commit_block_hash = parent_of_locked_block_qc.block_hash
                            self.execute_block(commit_block_hash)
                    except IndexError:
                        print(f"HotStuffNode {self.node_id} (Leader): Could not find block in local chain to check for commit.")


    def execute_block(self, block_hash: str):
        """
        Placeholder for executing a committed block.
        In a real implementation, this would apply the block's transactions to the state machine.
        For now, it just prints a confirmation.
        """
        # This logic needs to be more robust. How does a node find the block by hash?
        # We need a block store/lookup. For now, we search the local chain.
        block_to_execute = None
        for block in reversed(self.blockchain.chain):
            if block.hash == block_hash:
                block_to_execute = block
                break

        if block_to_execute:
             print(f"HotStuffNode {self.node_id}: **COMMITTING & EXECUTING** block {block_to_execute.index} with hash {block_hash[:8]}...")
             # Here, we would tell the benchmark runner that a block and its transactions are finalized.
             # This needs a proper implementation in the HotStuffAdapter.
        else:
            print(f"HotStuffNode {self.node_id}: Could not find block with hash {block_hash[:8]} to execute.")


if __name__ == '__main__':
    print("--- HotStuffNode Basic Test ---")
    node_ids = ["N0", "N1", "N2", "N3"]

    # Mock blockchain (we need at least a genesis block)
    mock_bc = Blockchain(genesis_sealer_id="hotstuff_genesis")

    # Create a node
    node0 = HotStuffNode("N0", node_ids, mock_bc)

    # Test proposal creation (as leader)
    node0.pending_client_requests.append({"tx_data": "some_transaction"})
    proposal_block = node0.create_proposal()
    print(f"\nLeader created a proposal block: {proposal_block}")
    assert proposal_block.qc is None # First proposal has no parent QC

    # Test vote reception
    mock_vote = VoteMessage(block_hash=proposal_block.hash, signature_share="N1")
    node0.on_receive_vote(mock_vote)
    mock_vote2 = VoteMessage(block_hash=proposal_block.hash, signature_share="N2")
    node0.on_receive_vote(mock_vote2)

    # With N=4, quorum is 3. We need one more vote.
    assert node0.prepare_qc is None
    mock_vote3 = VoteMessage(block_hash=proposal_block.hash, signature_share="N3")
    node0.on_receive_vote(mock_vote3)

    assert node0.prepare_qc is not None, "Leader should have created a QC after quorum."
    assert node0.prepare_qc.block_hash == proposal_block.hash
    assert len(node0.prepare_qc.signatures) == 3

    print("\n--- HotStuff Basic Test Finished ---")
