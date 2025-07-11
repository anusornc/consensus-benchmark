# Basic Network Simulator for PBFT and other distributed consensus protocols

class NetworkSimulator:
    """
    A simple in-memory network simulator for routing messages between registered nodes.

    This simulator allows nodes to "broadcast" messages to all other nodes or "send"
    messages to specific nodes. It directly calls the appropriate message handler
    method on the recipient node instance(s). This is intended for single-process
    simulations of distributed consensus protocols.

    Attributes:
        nodes (dict): A dictionary mapping node_id (str) to node_instance objects.
        trace_messages (bool): If True, prints details of messages being broadcast or sent.
    """
    def __init__(self, trace_messages: bool = True):
        """
        Initializes the NetworkSimulator.

        Args:
            trace_messages (bool, optional): Whether to print message routing details.
                                             Defaults to True.
        """
        self.nodes = {}  # node_id: node_instance
        # self.message_queue = [] # Optional: for simulating network latency or ordered delivery (not implemented)
        self.trace_messages = trace_messages

    def register_node(self, node_instance: object):
        """
        Registers a node instance with the simulator.

        The node instance must have a `node_id` attribute. It's also expected
        to have message handler methods like `handle_pre_prepare`, `handle_prepare`,
        `handle_commit` for PBFT, and a `set_network_simulator` method to receive
        a reference to this simulator instance.

        Args:
            node_instance (object): The node object to register.

        Raises:
            ValueError: If the node instance does not have a `node_id` attribute.
        """
        if not hasattr(node_instance, 'node_id'):
            raise ValueError("Node instance must have a 'node_id' attribute.")

        # Optional: Check for expected handler methods for robustness, e.g., for PBFT
        # if not (hasattr(node_instance, 'handle_pre_prepare') and callable(getattr(node_instance, 'handle_pre_prepare'))):
        #     print(f"Warning: Node {node_instance.node_id} does not have 'handle_pre_prepare' method.")

        self.nodes[node_instance.node_id] = node_instance
        if hasattr(node_instance, 'set_network_simulator') and callable(getattr(node_instance, 'set_network_simulator')):
            node_instance.set_network_simulator(self)
        if self.trace_messages: # Becomes less noisy if trace_messages is False
            print(f"NetworkSimulator: Node {node_instance.node_id} registered.")

    def broadcast(self, sender_id: str, message: object):
        """
        Simulates broadcasting a message from a sender to all other registered nodes.

        The simulator directly calls the appropriate handler method on each recipient node
        based on the message's class name (e.g., `handle_pre_prepare` for `PrePrepareMessage`).

        Args:
            sender_id (str): The ID of the node initiating the broadcast.
            message (object): The message object to broadcast. It should have a `__class__.__name__`
                              that corresponds to expected message types (e.g., "PrePrepareMessage").
        """
        if sender_id not in self.nodes:
            print(f"NetworkSimulator Error: Sender node {sender_id} not registered.")
            return

        if self.trace_messages:
            # Attempt to get seq_num and view for more informative tracing, default to 'N/A' if not present
            seq_num_str = getattr(message, 'seq_num', 'N/A')
            view_str = getattr(message, 'view', 'N/A')
            digest_str = getattr(message, 'request_digest', 'N/A')
            if isinstance(digest_str, str): digest_str = digest_str[:8] + "..."

            print(f"NetworkSimulator: Node {sender_id} broadcasting {message.__class__.__name__} (V:{view_str}, S:{seq_num_str}, D:{digest_str})")

        for node_id, node_instance in self.nodes.items():
            if node_id == sender_id: # Typically, nodes don't broadcast to themselves in this manner
                continue

            self._dispatch_message(node_instance, message, sender_id)

    def send(self, sender_id: str, recipient_id: str, message: object):
        """
        Simulates sending a message from a sender to a specific recipient node.

        The simulator directly calls the appropriate handler method on the recipient node.

        Args:
            sender_id (str): The ID of the node sending the message.
            recipient_id (str): The ID of the node to receive the message.
            message (object): The message object to send.
        """
        if sender_id not in self.nodes:
            print(f"NetworkSimulator Error: Sender node {sender_id} not registered.")
            return
        if recipient_id not in self.nodes:
            print(f"NetworkSimulator Error: Recipient node {recipient_id} not registered.")
            return

        recipient_node = self.nodes[recipient_id]
        if self.trace_messages:
            seq_num_str = getattr(message, 'seq_num', 'N/A')
            view_str = getattr(message, 'view', 'N/A')
            digest_str = getattr(message, 'request_digest', 'N/A')
            if isinstance(digest_str, str): digest_str = digest_str[:8] + "..."
            print(f"NetworkSimulator: Node {sender_id} sending {message.__class__.__name__} to {recipient_id} (V:{view_str}, S:{seq_num_str}, D:{digest_str})")

        self._dispatch_message(recipient_node, message, sender_id)

    def _dispatch_message(self, recipient_node_instance: object, message: object, original_sender_id: str):
        """
        Internal helper to dispatch a message to the correct handler on the recipient node.
        It determines the handler method based on the message's class name.

        Args:
            recipient_node_instance (object): The recipient node object.
            message (object): The message to dispatch.
            original_sender_id (str): The ID of the node that initiated the send/broadcast.
                                      (The `message` object itself also has a `sender_id` field
                                       indicating who crafted the message, which might be different
                                       if messages are forwarded, though not used in this dispatch).
        """
        msg_type_name = message.__class__.__name__
        handler_method_name = None

        # Map message class names to handler method names
        if msg_type_name == "PrePrepareMessage":
            handler_method_name = "handle_pre_prepare"
        elif msg_type_name == "PrepareMessage":
            handler_method_name = "handle_prepare"
        elif msg_type_name == "CommitMessage":
            handler_method_name = "handle_commit"
        # Extend with other message types and their handlers as needed:
        # elif msg_type_name == "ViewChangeMessage":
        #     handler_method_name = "handle_view_change"
        # elif msg_type_name == "ClientRequestMessage": # If routing client requests through simulator
        #     handler_method_name = "handle_client_request"

        if handler_method_name:
            handler_method = getattr(recipient_node_instance, handler_method_name, None)
            if handler_method and callable(handler_method):
                try:
                    if msg_type_name == "ClientRequestMessage" and handler_method_name == "handle_client_request":
                        # PBFTNode.handle_client_request expects payload, not the message object
                        handler_method(message.operation)
                    else:
                        handler_method(message)
                except Exception as e:
                    print(f"NetworkSimulator Error: Exception dispatching {msg_type_name} to {recipient_node_instance.node_id} from {original_sender_id}. Error: {e}")
                    # import traceback; traceback.print_exc() # For detailed debugging
            else:
                print(f"NetworkSimulator Warning: Node {recipient_node_instance.node_id} has no method '{handler_method_name}' for message type {msg_type_name}.")
        else:
            print(f"NetworkSimulator Warning: No handler defined for message type {msg_type_name} for node {recipient_node_instance.node_id} (from {original_sender_id}).")


if __name__ == '__main__':
    # Dummy Node class for testing the simulator
    class MockPBFTNode:
        def __init__(self, node_id: str):
            self.node_id = node_id
            self.received_messages = []
            self.simulator_ref = None

        def set_network_simulator(self, sim):
            self.simulator_ref = sim
            # print(f"MockNode {self.node_id}: Network simulator set.") # Less verbose for test

        def handle_pre_prepare(self, msg):
            print(f"MockNode {self.node_id}: Received PrePrepare from {msg.sender_id}: Digest {msg.request_digest[:8]}...")
            self.received_messages.append(msg)

        def handle_prepare(self, msg):
            print(f"MockNode {self.node_id}: Received Prepare from {msg.sender_id}: Digest {msg.request_digest[:8]}...")
            self.received_messages.append(msg)

        def handle_commit(self, msg):
            print(f"MockNode {self.node_id}: Received Commit from {msg.sender_id}: Digest {msg.request_digest[:8]}...")
            self.received_messages.append(msg)

        def broadcast_something(self, message_to_send):
            if self.simulator_ref:
                # print(f"MockNode {self.node_id}: Initiating broadcast of {message_to_send.__class__.__name__}")
                self.simulator_ref.broadcast(self.node_id, message_to_send)

    # Import message types for testing (assuming they are in a sibling 'consensus' directory)
    try:
        from consensus.pbft_messages import PrePrepareMessage, PrepareMessage
    except ImportError: # Fallback for running file directly for testing
        class PrePrepareMessage:
            def __init__(self, view, seq_num, sender_id, request_digest, request_payload):
                self.view, self.seq_num, self.sender_id, self.request_digest, self.request_payload = view, seq_num, sender_id, request_digest, request_payload
                self.__class__.__name__ = "PrePrepareMessage" # Mock class name for dispatch
        class PrepareMessage:
            def __init__(self, view, seq_num, sender_id, request_digest):
                self.view, self.seq_num, self.sender_id, self.request_digest = view, seq_num, sender_id, request_digest
                self.__class__.__name__ = "PrepareMessage"


    sim = NetworkSimulator(trace_messages=True)

    node_A = MockPBFTNode("A")
    node_B = MockPBFTNode("B")
    node_C = MockPBFTNode("C")

    sim.register_node(node_A)
    sim.register_node(node_B)
    sim.register_node(node_C)

    print("\n--- Testing Broadcast ---")
    pre_prepare_test_msg = PrePrepareMessage(view=0, seq_num=1, sender_id="A",
                                             request_digest="test_digest_preprepare_example", request_payload={"data":"test_payload"})
    node_A.broadcast_something(pre_prepare_test_msg)

    assert len(node_B.received_messages) == 1 and isinstance(node_B.received_messages[0], PrePrepareMessage)
    assert len(node_C.received_messages) == 1 and isinstance(node_C.received_messages[0], PrePrepareMessage)
    assert len(node_A.received_messages) == 0

    print("\n--- Testing Send (Direct Message) ---")
    prepare_test_msg = PrepareMessage(view=0, seq_num=1, sender_id="B", request_digest="test_digest_preprepare_example")
    if node_B.simulator_ref:
        node_B.simulator_ref.send(sender_id="B", recipient_id="C", message=prepare_test_msg)

    assert len(node_C.received_messages) == 2 and isinstance(node_C.received_messages[1], PrepareMessage)

    print("\nNetworkSimulator basic test finished.")
