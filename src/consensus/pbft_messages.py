import time # For __main__ example timestamping
# Defines the message structures used in the PBFT protocol.

class PBFTMessage:
    """
    Base class for all PBFT messages.

    Attributes:
        message_type (str): The type of the PBFT message (e.g., "PRE-PREPARE", "PREPARE").
        view (int): The view number in which this message was generated.
        seq_num (int): The sequence number assigned to the request this message pertains to.
        sender_id (str): The unique identifier of the node that sent this message.
    """
    def __init__(self, message_type: str, view: int, seq_num: int, sender_id: str):
        """
        Initializes a PBFTMessage.

        Args:
            message_type (str): Type of the PBFT message.
            view (int): View number.
            seq_num (int): Sequence number.
            sender_id (str): ID of the sending node.
        """
        self.message_type = message_type
        self.view = view
        self.seq_num = seq_num
        self.sender_id = sender_id

    def __repr__(self) -> str:
        """Returns a string representation of the PBFT message."""
        return (f"{self.message_type}(View: {self.view}, Seq: {self.seq_num}, "
                f"Sender: {self.sender_id})")

class PrePrepareMessage(PBFTMessage):
    """
    Represents a PRE-PREPARE message in the PBFT protocol.
    Sent by the primary replica to backups to initiate consensus on a client request.
    Corresponds to the PBFT protocol message: `<PRE-PREPARE, v, n, d(m), m>`

    Attributes:
        request_digest (str): The digest (hash) of the client request message `m`.
        request_payload (any): The actual client request message `m` (e.g., a list of transactions).
    """
    def __init__(self, view: int, seq_num: int, sender_id: str, request_digest: str, request_payload: any):
        """
        Initializes a PrePrepareMessage.

        Args:
            view (int): The current view number.
            seq_num (int): The sequence number for this request.
            sender_id (str): The ID of the primary replica sending this message.
            request_digest (str): The digest of the client request.
            request_payload (any): The client request payload.
        """
        super().__init__("PRE-PREPARE", view, seq_num, sender_id)
        self.request_digest = request_digest
        self.request_payload = request_payload

    def __repr__(self) -> str:
        """Returns a string representation of the PrePrepareMessage."""
        payload_str = str(self.request_payload)
        return (f"PrePrepare(View: {self.view}, Seq: {self.seq_num}, Sender: {self.sender_id}, "
                f"Digest: {self.request_digest[:8]}..., Payload: {payload_str[:50]}{'...' if len(payload_str) > 50 else ''})")

class PrepareMessage(PBFTMessage):
    """
    Represents a PREPARE message in the PBFT protocol.
    Sent by backup replicas after they receive and validate a PRE-PREPARE message.
    Corresponds to the PBFT protocol message: `<PREPARE, v, n, d(m), i>`

    Attributes:
        request_digest (str): The digest of the client request message, matching the one in PRE-PREPARE.
    """
    def __init__(self, view: int, seq_num: int, sender_id: str, request_digest: str):
        """
        Initializes a PrepareMessage.

        Args:
            view (int): The current view number.
            seq_num (int): The sequence number for this request.
            sender_id (str): The ID of the replica sending this PREPARE message.
            request_digest (str): The digest of the client request.
        """
        super().__init__("PREPARE", view, seq_num, sender_id)
        self.request_digest = request_digest

    def __repr__(self) -> str:
        """Returns a string representation of the PrepareMessage."""
        return (f"Prepare(View: {self.view}, Seq: {self.seq_num}, Sender: {self.sender_id}, "
                f"Digest: {self.request_digest[:8]}...)")

class CommitMessage(PBFTMessage):
    """
    Represents a COMMIT message in the PBFT protocol.
    Sent by replicas once they have "prepared" a request (i.e., received enough matching PREPARE messages).
    Corresponds to the PBFT protocol message: `<COMMIT, v, n, d(m), i>`

    Attributes:
        request_digest (str): The digest of the client request message.
    """
    def __init__(self, view: int, seq_num: int, sender_id: str, request_digest: str):
        """
        Initializes a CommitMessage.

        Args:
            view (int): The current view number.
            seq_num (int): The sequence number for this request.
            sender_id (str): The ID of the replica sending this COMMIT message.
            request_digest (str): The digest of the client request.
        """
        super().__init__("COMMIT", view, seq_num, sender_id)
        self.request_digest = request_digest

    def __repr__(self) -> str:
        """Returns a string representation of the CommitMessage."""
        return (f"Commit(View: {self.view}, Seq: {self.seq_num}, Sender: {self.sender_id}, "
                f"Digest: {self.request_digest[:8]}...)")

class ClientRequestMessage:
    """
    Represents a request message sent from a client to the PBFT replicas.
    This is a simplified structure for simulation purposes.

    Attributes:
        client_id (str): Identifier for the client sending the request.
        timestamp (float): Timestamp of when the request was created by the client.
        operation (any): The actual operation or data payload of the request
                         (e.g., a transaction dictionary or a list of transactions).
        request_id (str): A unique identifier for this client request.
    """
    def __init__(self, client_id: str, timestamp: float, operation: any, request_id: str):
        """
        Initializes a ClientRequestMessage.

        Args:
            client_id (str): ID of the client.
            timestamp (float): Creation timestamp of the request.
            operation (any): The request payload/operation.
            request_id (str): Unique ID for the request.
        """
        self.client_id = client_id
        self.timestamp = timestamp
        self.operation = operation
        self.request_id = request_id

    def __repr__(self) -> str:
        """Returns a string representation of the ClientRequestMessage."""
        op_str = str(self.operation)
        return f"ClientRequest(ID: {self.request_id}, Client: {self.client_id}, Op: {op_str[:50]}{'...' if len(op_str) > 50 else ''})"


if __name__ == '__main__':
    pre_prepare_msg = PrePrepareMessage(view=1, seq_num=100, sender_id="node_P",
                                        request_digest="digest123abc", request_payload=["tx1", "tx2"])
    print(pre_prepare_msg)

    prepare_msg = PrepareMessage(view=1, seq_num=100, sender_id="node_B1", request_digest="digest123abc")
    print(prepare_msg)

    commit_msg = CommitMessage(view=1, seq_num=100, sender_id="node_B1", request_digest="digest123abc")
    print(commit_msg)

    client_req = ClientRequestMessage(client_id="clientA", timestamp=time.time(),
                                      operation={"action": "transfer", "amount": 100, "to": "Bob"},
                                      request_id="req001-xyz")
    print(client_req)
