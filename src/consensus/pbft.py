import time
import hashlib
import json
from collections import defaultdict

from src.blockchain.blockchain import Blockchain # For type hinting Blockchain instances
from src.blockchain.block import Block # For type hinting Block instances
from src.transactions.transaction import Transaction # For type hinting Transaction objects if needed
from .pbft_messages import PBFTMessage, PrePrepareMessage, PrepareMessage, CommitMessage # Added PBFTMessage

# Helper for creating digests
def create_digest(payload: any) -> str:
    """
    Creates a SHA256 digest for a given payload.
    The payload is first serialized to a JSON string with sorted keys for consistency.

    Args:
        payload (any): The data to digest (e.g., a list of transactions, a dictionary).

    Returns:
        str: The hexadecimal string representation of the SHA256 hash.
    """
    payload_string = json.dumps(payload, sort_keys=True).encode('utf-8')
    return hashlib.sha256(payload_string).hexdigest()

class PBFTNode:
    """
    Represents a single node participating in the PBFT consensus protocol.

    Each node maintains its own state, including the current view, sequence number,
    a log of received messages, and its own instance of the blockchain.
    Nodes communicate via a network simulator.

    Attributes:
        node_id (str): Unique identifier for this node.
        all_node_ids (list[str]): Sorted list of all node IDs in the network.
        blockchain (Blockchain): This node's local instance of the blockchain.
        n (int): Total number of nodes in the network.
        f (int): Maximum number of faulty nodes the system can tolerate (n = 3f + 1).
        current_view (int): The current view number this node is in.
        current_seq_num (int): The last sequence number processed or assigned by this node.
        message_log (defaultdict): Stores PBFT messages (pre-prepare, prepares, commits)
                                   and request details, indexed by view and sequence number.
        pending_client_requests (list): Queue for client request payloads if this node is primary.
        network_simulator (NetworkSimulator): Reference to the network simulation environment.
        primary_id (str): The ID of the current primary node for the current_view.
        is_primary_node (bool): True if this node is the current primary, False otherwise.
    """
    def __init__(self, node_id: str, all_node_ids: list[str], blockchain: Blockchain):
        """
        Initializes a PBFTNode.

        Args:
            node_id (str): The unique identifier for this node.
            all_node_ids (list[str]): A list of all unique node identifiers in the network.
            blockchain (Blockchain): The blockchain instance this node will use.
        """
        self.node_id = node_id
        self.all_node_ids = sorted(list(set(all_node_ids)))
        self.blockchain = blockchain

        self.n = len(self.all_node_ids)
        self.f = (self.n - 1) // 3

        self.current_view = 0
        self.current_seq_num = 0

        self.message_log = defaultdict(lambda: defaultdict(lambda: {
            'pre_prepare': None,    # Stores the PrePrepareMessage object
            'prepares': {},         # sender_id: PrepareMessage object
            'commits': {},          # sender_id: CommitMessage object
            'request_payload': None,# The actual client request data
            'request_digest': None, # Digest of the request_payload
            'status': 'pending'     # Lifecycle: pending -> pre-preparing -> pre-prepared -> prepared -> committed -> executed
        }))

        self.pending_client_requests = []
        self.network_simulator = None
        self.buffered_messages = defaultdict(list) # Buffer for out-of-order messages: (view, seq_num) -> [message]

        self.update_primary()

        print(f"Node {self.node_id} initialized. N={self.n}, f={self.f}. Primary for view {self.current_view}: {self.primary_id}. I am primary: {self.is_primary_node}")

    def set_network_simulator(self, simulator):
        """
        Sets the network simulator for this node, enabling message broadcast and send.

        Args:
            simulator: An instance of a network simulator (e.g., NetworkSimulator).
        """
        self.network_simulator = simulator
        # print(f"Node {self.node_id}: Network simulator has been set.") # Less verbose

    def update_primary(self):
        """
        Determines and updates the primary node ID for the current view.
        The primary is selected cyclically based on `current_view % n`.
        Also updates `self.is_primary_node`.
        """
        self.primary_id = self.all_node_ids[self.current_view % self.n]
        self.is_primary_node = (self.node_id == self.primary_id)


    def is_primary(self) -> bool:
        """Returns True if this node is the current primary, False otherwise."""
        return self.is_primary_node

    def broadcast_message(self, message: PBFTMessage):
        """
        Broadcasts a PBFT message to all other nodes using the network simulator.
        If no simulator is set, it logs that the broadcast is not sent.

        Args:
            message (PBFTMessage): The PBFT message object to broadcast.
        """
        if self.network_simulator:
            if self.network_simulator.trace_messages:
                 print(f"Node {self.node_id} (V:{self.current_view}): Broadcasting {message.__class__.__name__} for S:{getattr(message, 'seq_num', 'N/A')}, D:{getattr(message, 'request_digest', 'N/A')[:8]}")
            self.network_simulator.broadcast(self.node_id, message)
        else:
            print(f"Node {self.node_id} (V:{self.current_view}): [No Network Sim] Broadcasting {message.__class__.__name__} for S:{getattr(message, 'seq_num', 'N/A')}")


    def send_message(self, recipient_id: str, message: PBFTMessage):
        """
        Sends a PBFT message to a specific recipient node using the network simulator.
        If no simulator is set, it logs that the message is not sent.

        Args:
            recipient_id (str): The ID of the node to send the message to.
            message (PBFTMessage): The PBFT message object to send.
        """
        if self.network_simulator:
            if self.network_simulator.trace_messages:
                print(f"Node {self.node_id} (V:{self.current_view}): Sending {message.__class__.__name__} to {recipient_id} (S:{getattr(message, 'seq_num', 'N/A')})")
            self.network_simulator.send(self.node_id, recipient_id, message)
        else:
            print(f"Node {self.node_id} (V:{self.current_view}): [No Network Sim] Sending {message} to {recipient_id}")


    def handle_client_request(self, request_payload: any):
        """
        Handles a new client request payload.
        If this node is the primary, it adds the request to its pending queue and
        initiates the pre-prepare phase if it's the only pending request.
        If not primary, it currently logs this fact (forwarding logic could be added).

        Args:
            request_payload (any): The payload of the client's request (e.g., a transaction
                                   dictionary or a list of such dictionaries).
        """
        print(f"Node {self.node_id} (V:{self.current_view}): Received client request payload: {str(request_payload)[:100]}...")
        if not self.is_primary():
            print(f"Node {self.node_id}: Not primary for view {self.current_view}. Current primary: {self.primary_id}. Request not processed by this node.")
            return

        self.pending_client_requests.append(request_payload)
        if len(self.pending_client_requests) == 1: # Process immediately if it's the first in queue
             self.initiate_pre_prepare()


    def initiate_pre_prepare(self):
        """
        Initiates the PRE-PREPARE phase if this node is the primary and has pending client requests.
        It assigns a sequence number, creates a digest for the request, logs it,
        and broadcasts a `PrePrepareMessage` to all other replicas.
        """
        if not self.is_primary() or not self.pending_client_requests:
            return

        request_payload = self.pending_client_requests.pop(0)

        self.current_seq_num += 1
        seq_num = self.current_seq_num
        view = self.current_view
        request_digest = create_digest(request_payload)

        log_entry = self.message_log[view][seq_num]
        if log_entry['status'] != 'pending':
            print(f"Node {self.node_id} (Primary, V:{view}): Seq num {seq_num} already has status {log_entry['status']}. Race/Re-proposal? Halting pre-prepare.")
            self.current_seq_num -= 1
            self.pending_client_requests.insert(0, request_payload) # Re-queue
            return

        log_entry['request_payload'] = request_payload
        log_entry['request_digest'] = request_digest
        log_entry['status'] = 'pre-preparing'

        pre_prepare_msg = PrePrepareMessage(
            view=view,
            seq_num=seq_num,
            sender_id=self.node_id,
            request_digest=request_digest,
            request_payload=request_payload
        )
        print(f"Node {self.node_id} (Primary, V:{view}): Initiating PRE-PREPARE for S:{seq_num}, D:{request_digest[:8]}")
        self.broadcast_message(pre_prepare_msg)


    def handle_pre_prepare(self, pre_prepare_msg: PrePrepareMessage):
        """
        Handles an incoming `PrePrepareMessage` (typically for backup replicas).
        Validates the message (view, sender, digest). If valid and new, logs it,
        sets status to 'pre-prepared', broadcasts a `PrepareMessage`, and processes its own Prepare.

        Args:
            pre_prepare_msg (PrePrepareMessage): The received pre-prepare message.
        """
        print(f"Node {self.node_id} (V:{self.current_view}): Handling PRE-PREPARE from {pre_prepare_msg.sender_id} for V:{pre_prepare_msg.view}, S:{pre_prepare_msg.seq_num}, D:{pre_prepare_msg.request_digest[:8]}")

        view = pre_prepare_msg.view
        seq_num = pre_prepare_msg.seq_num
        digest = pre_prepare_msg.request_digest
        sender_id = pre_prepare_msg.sender_id

        if view < self.current_view:
            print(f"Node {self.node_id}: PRE-PREPARE from past view {view}. Current view {self.current_view}. Ignoring.")
            return
        if view == self.current_view and sender_id != self.primary_id:
            print(f"Node {self.node_id}: PRE-PREPARE not from current primary {self.primary_id} for view {view}. Sender: {sender_id}. Ignoring.")
            return
        if view > self.current_view:
            print(f"Node {self.node_id}: PRE-PREPARE from future view {view}. Current view {self.current_view}. Buffering/Ignoring.")
            # TODO: Implement buffering for future view messages or view synchronization.
            return

        log_entry = self.message_log[view][seq_num]
        calculated_digest = create_digest(pre_prepare_msg.request_payload)
        if calculated_digest != digest:
            print(f"Node {self.node_id}: PRE-PREPARE digest mismatch for V:{view}, S:{seq_num}. Expected {calculated_digest[:8]}, got {digest[:8]}. Ignoring.")
            return

        if log_entry['pre_prepare'] is not None: # If already have a pre-prepare for this v,s
            if log_entry['request_digest'] != digest: # ...and it's conflicting
                print(f"Node {self.node_id}: Conflicting PRE-PREPARE for V:{view}, S:{seq_num}. Existing D:{log_entry['request_digest'][:8]}, New D:{digest[:8]}. Ignoring new.")
                # TODO: Potentially trigger view change if primary is Byzantine.
                return
            else: # Duplicate valid pre-prepare
                print(f"Node {self.node_id}: Duplicate valid PRE-PREPARE for V:{view}, S:{seq_num}. Status: {log_entry['status']}.")
                if log_entry['status'] not in ['pending', 'pre-prepared']: return # Already moved past this stage

        if log_entry['status'] == 'pending' or log_entry['pre_prepare'] is None :
            log_entry['pre_prepare'] = pre_prepare_msg
            log_entry['request_payload'] = pre_prepare_msg.request_payload
            log_entry['request_digest'] = digest
            log_entry['status'] = 'pre-prepared'

            prepare_msg = PrepareMessage(
                view=view, seq_num=seq_num, sender_id=self.node_id, request_digest=digest
            )
            print(f"Node {self.node_id} (V:{view}): Broadcasting PREPARE for S:{seq_num}, D:{digest[:8]}")
            self.broadcast_message(prepare_msg)
            self.handle_prepare(prepare_msg) # Process own prepare message

            # Process buffered messages for this (view, seq_num)
            buffered_key = (view, seq_num)
            if buffered_key in self.buffered_messages:
                for buffered_msg in self.buffered_messages[buffered_key]:
                    print(f"Node {self.node_id} (V:{view}, S:{seq_num}): Processing buffered {buffered_msg.__class__.__name__} from {buffered_msg.sender_id}")
                    if isinstance(buffered_msg, PrepareMessage):
                        self.handle_prepare(buffered_msg)
                    elif isinstance(buffered_msg, CommitMessage):
                        self.handle_commit(buffered_msg)
                del self.buffered_messages[buffered_key]

        elif log_entry['request_digest'] == digest and log_entry['status'] == 'pre-prepared':
            print(f"Node {self.node_id}: Received duplicate valid PRE-PREPARE for V:{view}, S:{seq_num} while status is 'pre-prepared'.")
        else: # Should not happen if logic is correct
            print(f"Node {self.node_id}: PRE-PREPARE for V:{view}, S:{seq_num} in unexpected state. Status: {log_entry['status']}, Known D:{log_entry['request_digest'][:8] if log_entry['request_digest'] else 'None'}.")


    def handle_prepare(self, prepare_msg: PrepareMessage):
        """
        Handles an incoming `PrepareMessage`.
        Validates the message, stores it. If the node collects enough (2f) matching
        Prepare messages (plus its own implicit agreement via pre-prepare or its own Prepare),
        it transitions to the 'prepared' state and broadcasts a `CommitMessage`.

        Args:
            prepare_msg (PrepareMessage): The received prepare message.
        """
        print(f"Node {self.node_id} (V:{self.current_view}): Handling PREPARE from {prepare_msg.sender_id} for V:{prepare_msg.view}, S:{prepare_msg.seq_num}, D:{prepare_msg.request_digest[:8]}")

        view = prepare_msg.view
        seq_num = prepare_msg.seq_num
        digest = prepare_msg.request_digest
        sender_id = prepare_msg.sender_id

        if view < self.current_view or view > self.current_view: # Ignore messages not for current view
            print(f"Node {self.node_id}: PREPARE for view {view} (current: {self.current_view}). Ignoring.")
            return

        log_entry = self.message_log[view][seq_num]

        if log_entry['request_digest'] is None:
            # If this node is not the primary for this message's view/seq (or it's a future op for current primary),
            # it means pre-prepare hasn't been processed locally. Buffer the Prepare.
            # Primary node should have request_digest set if it initiated this seq_num in this view.
            is_primary_for_this_op = (self.node_id == self.all_node_ids[view % self.n] and view == self.current_view and seq_num == self.current_seq_num)
            if not is_primary_for_this_op :
                print(f"Node {self.node_id} (V:{self.current_view}): No request_digest for V:{view}, S:{seq_num} (Pre-prepare not processed). Buffering PREPARE from {sender_id}.")
                self.buffered_messages[(view, seq_num)].append(prepare_msg)
                return
            elif is_primary_for_this_op and log_entry['status'] != 'pre-preparing':
                 # This case implies primary initiated an operation but its own log_entry is not yet 'pre-preparing'
                 # which is unexpected. It might happen if there's a race or internal logic error for primary.
                 print(f"Node {self.node_id} (Primary V:{self.current_view}): Warning - request_digest is None for its own operation V:{view},S:{seq_num} when PREPARE from {sender_id} arrived. Status: {log_entry['status']}. Ignoring PREPARE.")
                 return
            # If it *is* primary for this op and digest is None but status *is* 'pre-preparing', then it's an internal error.
            # This should have been set in initiate_pre_prepare.

        if log_entry['request_digest'] != digest: # This check is vital once request_digest is confirmed to be set.
            print(f"Node {self.node_id} (V:{self.current_view}): PREPARE digest mismatch for V:{view}, S:{seq_num}. Expected {log_entry['request_digest'][:8]}, got {digest[:8]}. Ignoring PREPARE from {sender_id}.")
            return

        if sender_id not in log_entry['prepares']:
            log_entry['prepares'][sender_id] = prepare_msg
        else:
            print(f"Node {self.node_id}: Duplicate PREPARE from {sender_id} for V:{view},S:{seq_num},D:{digest[:8]}.")
            return

        # Check for Prepared Predicate
        # A replica is "prepared" if it has:
        #   1. The request payload logged.
        #   2. A valid pre-prepare from the primary (or is the primary who initiated it).
        #   3. 2f valid prepare messages from distinct replicas (can include itself if backup) that match the pre-prepare.
        has_request_and_pre_prepare_info = (log_entry['request_payload'] is not None and
                                           log_entry['request_digest'] == digest and
                                           (log_entry['pre_prepare'] is not None or self.is_primary()))

        if not has_request_and_pre_prepare_info:
            print(f"Node {self.node_id}: Cannot evaluate prepared predicate for V:{view},S:{seq_num}. Missing request/pre-prepare info.")
            return

        num_matching_prepares = len(log_entry['prepares'])

        # Threshold: 2f matching Prepare messages.
        # - Primary needs 2f from backups.
        # - Backup needs its own Prepare + (2f-1) from others = 2f total Prepare messages in its log.

        # Determine the correct status before checking threshold for "prepared"
        # For primary, its own request starts as 'pre-preparing'. For backups, it's 'pre-prepared' after PrePrepare.
        eligible_to_prepare = False
        # Check for primary:
        # Primary's log_entry for its own request will have 'request_digest' and status 'pre-preparing'.
        # It does not store its own PrePrepareMessage object in log_entry['pre_prepare'].
        # It needs 2f matching prepares from backups.
        # num_matching_prepares counts messages in log_entry['prepares'], which are from other nodes.
        if self.is_primary() and log_entry['status'] == 'pre-preparing' and log_entry['request_digest'] == digest:
            if num_matching_prepares >= (2 * self.f):
                eligible_to_prepare = True
        # Check for backups:
        # Backup's log_entry status becomes 'pre-prepared' after validating the PrePrepare.
        # It needs 2f matching prepares (its own + 2f-1 others). num_matching_prepares includes its own.
        elif not self.is_primary() and log_entry['status'] == 'pre-prepared' and \
             log_entry['pre_prepare'] is not None and log_entry['pre_prepare'].request_digest == digest:
            if num_matching_prepares >= (2 * self.f): # This count includes its own prepare
                eligible_to_prepare = True

        if eligible_to_prepare:
            log_entry['status'] = 'prepared'
            print(f"Node {self.node_id} (V:{view}): **PREPARED** for S:{seq_num}, D:{digest[:8]} with {num_matching_prepares} prepares.")

            commit_msg = CommitMessage(
                view=view, seq_num=seq_num, sender_id=self.node_id, request_digest=digest
            )
            print(f"Node {self.node_id} (V:{view}): Broadcasting COMMIT for S:{seq_num}, D:{digest[:8]}")
            self.broadcast_message(commit_msg)
            self.handle_commit(commit_msg) # Process own commit
        elif log_entry['status'] == 'pre-prepared' or (self.is_primary() and log_entry['status'] == 'pre-preparing'):
             # Not enough prepares yet, but still in a valid prior state
             pass


    def handle_commit(self, commit_msg: CommitMessage):
        """
        Handles an incoming `CommitMessage`.
        Validates the message, stores it. If the node is 'prepared' and collects
        2f+1 matching Commit messages (including its own), it transitions to 'committed'
        and executes the request.

        Args:
            commit_msg (CommitMessage): The received commit message.
        """
        print(f"Node {self.node_id} (V:{self.current_view}): Handling COMMIT from {commit_msg.sender_id} for V:{commit_msg.view}, S:{commit_msg.seq_num}, D:{commit_msg.request_digest[:8]}")

        view = commit_msg.view
        seq_num = commit_msg.seq_num
        digest = commit_msg.request_digest
        sender_id = commit_msg.sender_id

        if view < self.current_view or view > self.current_view:
            print(f"Node {self.node_id}: COMMIT for view {view} (current: {self.current_view}). Ignoring.")
            return

        log_entry = self.message_log[view][seq_num]

        if log_entry['request_digest'] is None:
            # Similar to handle_prepare, buffer if pre-prepare/prepare phases haven't established the digest locally.
            # Primary should have this set from its own pre-prepare initiation.
            is_primary_for_this_op = (self.node_id == self.all_node_ids[view % self.n] and view == self.current_view and seq_num == self.current_seq_num)
            if not is_primary_for_this_op:
                print(f"Node {self.node_id} (V:{self.current_view}): No request_digest for V:{view}, S:{seq_num} (Not prepared). Buffering COMMIT from {sender_id}.")
                self.buffered_messages[(view, seq_num)].append(commit_msg)
                return
            elif is_primary_for_this_op and log_entry['status'] not in ['pre-preparing', 'prepared']:
                 print(f"Node {self.node_id} (Primary V:{self.current_view}): Warning - request_digest is None or not prepared for its own op V:{view},S:{seq_num} when COMMIT from {sender_id} arrived. Status: {log_entry['status']}. Ignoring COMMIT.")
                 return

        if log_entry['request_digest'] != digest:
            print(f"Node {self.node_id} (V:{self.current_view}): COMMIT digest mismatch for V:{view}, S:{seq_num}. Expected {log_entry['request_digest'][:8]}, got {digest[:8]}. Ignoring from {sender_id}.")
            return

        if sender_id not in log_entry['commits']:
             log_entry['commits'][sender_id] = commit_msg
        else:
            print(f"Node {self.node_id}: Duplicate COMMIT from {sender_id} for V:{view},S:{seq_num},D:{digest[:8]}.")
            return

        if log_entry['status'] == 'prepared':
            num_matching_commits = len(log_entry['commits'])
            if num_matching_commits >= (2 * self.f + 1): # 2f+1 threshold for commit
                log_entry['status'] = 'committed'
                print(f"Node {self.node_id} (V:{view}): **LOCALLY COMMITTED** for S:{seq_num}, D:{digest[:8]} with {num_matching_commits} commits.")
                self.execute_request(view, seq_num)
        elif log_entry['status'] in ['committed', 'executed']:
            pass # Already past this stage, ignore duplicate/late commits for state change
        else:
            print(f"Node {self.node_id}: Received COMMIT for V:{view},S:{seq_num}, but not 'prepared'. Status: {log_entry['status']}. Commit from {sender_id} logged.")


    def execute_request(self, view: int, seq_num: int):
        """
        Executes a request that has reached the 'committed' state.
        This involves creating a new block with the request's transactions and adding
        it to this node's local blockchain.
        Prevents re-execution of the same request.

        Args:
            view (int): The view in which the request was committed.
            seq_num (int): The sequence number of the committed request.
        """
        log_entry = self.message_log[view][seq_num]
        if log_entry['status'] != 'committed':
            print(f"Node {self.node_id}: Attempt to execute non-committed request V:{view}, S:{seq_num}. Status: {log_entry['status']}")
            return

        if getattr(log_entry, '_executed', False):
            print(f"Node {self.node_id}: Request V:{view}, S:{seq_num} already marked executed. Skipping.")
            return

        request_payload = log_entry['request_payload']
        block_sealer_id = f"pbft_v{view}_p{self.all_node_ids[view % self.n]}"

        transactions_for_block = []
        if isinstance(request_payload, list):
            transactions_for_block = request_payload
        elif isinstance(request_payload, dict):
            transactions_for_block = [request_payload]
        else:
            print(f"Node {self.node_id}: Unknown request payload format for block: {request_payload}")
            log_entry['status'] = 'execute_failed_payload'
            return

        prev_block = self.blockchain.last_block
        next_index = prev_block.index + 1 if prev_block else 0
        previous_hash_val = prev_block.hash if prev_block else "0"

        new_block = Block(
            index=next_index,
            transactions=transactions_for_block,
            timestamp=time.time(),
            previous_hash=previous_hash_val,
            nonce=seq_num,
            sealer_id=block_sealer_id
        )

        if self.blockchain.add_block(new_block):
            log_entry['status'] = 'executed'
            log_entry['_executed'] = True
            print(f"Node {self.node_id} (V:{view}): **EXECUTED** S:{seq_num}. Block {new_block.index} added with hash {new_block.hash[:8]}.")
        else:
            log_entry['status'] = 'execute_failed_blockchain_add'
            print(f"Node {self.node_id}: CRITICAL - FAILED TO EXECUTE V:{view}, S:{seq_num}. Blockchain rejected block {new_block.index}.")


    def initiate_view_change(self, new_view: int = None):
        """
        Initiates a view change process (currently highly simplified).
        In a full implementation, this involves broadcasting VIEW-CHANGE messages,
        collecting them, and the new primary broadcasting a NEW-VIEW message.
        This simplified version just advances the view number locally.

        Args:
            new_view (int, optional): If specified, attempts to change to this view number.
                                      Otherwise, increments the current view.
        """
        requested_view = new_view if new_view is not None else self.current_view + 1
        print(f"Node {self.node_id}: View change initiated for view {requested_view}. Current view: {self.current_view}")

        self.current_view = requested_view
        self.update_primary()
        print(f"Node {self.node_id}: Advanced to view {self.current_view}. New primary: {self.primary_id}. I am primary: {self.is_primary_node}")

        self.pending_client_requests.clear()

# Example usage will require a network simulator.
if __name__ == '__main__':
    node_ids_list = ["N0", "N1", "N2", "N3"]

    try:
        from src.transactions.transaction import Transaction # Full import for __main__
    except ImportError: # Fallback if run standalone or issues with path
        class Transaction:
            def __init__(self, client_id, app, data_dict): self.data = data_dict
            def to_dict(self): return self.data

    blockchains = {nid: Blockchain(genesis_sealer_id=f"pbft_genesis_{nid}") for nid in node_ids_list}
    pbft_nodes = {nid: PBFTNode(nid, node_ids_list, blockchains[nid]) for nid in node_ids_list}

    print(f"\n--- Simulating a Client Request to Primary ({pbft_nodes['N0'].primary_id} for view 0) ---")
    client_tx_op = Transaction("clientA", "app", {"action": "deposit", "amount": 100}).to_dict()

    pbft_nodes[pbft_nodes["N0"].primary_id].handle_client_request(client_tx_op)

    print("\n--- PBFT Node basic structure test complete (manual steps not shown for full flow) ---")
    print("Full PBFT flow requires running with the NetworkSimulator.")

    print("\n--- Testing Basic View Change (manual trigger on one node) ---")
    pbft_nodes["N0"].initiate_view_change()

    new_view_from_n0 = pbft_nodes["N0"].current_view
    for nid in node_ids_list:
        if nid != "N0":
            pbft_nodes[nid].current_view = new_view_from_n0
            pbft_nodes[nid].update_primary()

    print(f"After view change to {pbft_nodes['N0'].current_view}:")
    for nid in node_ids_list:
        print(f"Node {nid} primary status: {pbft_nodes[nid].is_primary()}, Believes primary is: {pbft_nodes[nid].primary_id}")

    expected_primary_after_vc = node_ids_list[new_view_from_n0 % len(node_ids_list)]
    assert pbft_nodes[expected_primary_after_vc].is_primary(), f"{expected_primary_after_vc} should be primary for view {new_view_from_n0}"
    if expected_primary_after_vc == "N1" and len(node_ids_list) == 4: # Specific check for N=4
         assert pbft_nodes["N1"].is_primary(), "N1 should be primary for view 1 when N=4"

    payload1 = {"tx": "A", "value": 10, "timestamp": 123.456}
    payload2 = {"timestamp": 123.456, "value": 10, "tx": "A"}
    payload3 = {"tx": "B", "value": 10, "timestamp": 123.456}
    assert create_digest(payload1) == create_digest(payload2), "Digests should match for equivalent payloads"
    assert create_digest(payload1) != create_digest(payload3), "Digests should differ for different payloads"
    print("\nDigest creation test passed.")
    print("PBFT __main__ tests completed.")
