import time
import json
from uuid import uuid4
# Corrected import names
from .provo_serializer import create_asset_creation_prov, create_asset_transfer_prov

class Transaction:
    """
    Represents a transaction in the blockchain.
    The core data payload of the transaction is structured as PROV-O compliant JSON-LD.

    Attributes:
        transaction_id (str): A unique identifier for the transaction (e.g., a UUID URN).
        timestamp (float): Unix timestamp indicating when the transaction object was created locally.
        transaction_type (str): Describes the nature of the transaction
                                (e.g., "asset_creation", "asset_transfer").
        asset_id_value (str): The main identifier of the asset involved (e.g., serial number).
        agent_ids (dict): Dictionary containing identifiers for agents involved, specific to the
                          transaction type (e.g., {"creator_agent_id": "id"},
                          {"from_agent_id": "id1", "to_agent_id": "id2"}).
        additional_props (dict): Any additional properties to be included in the PROV-O data.
        custom_base_uri (str): Base URI used for generating URNs/URIs within the PROV-O data.
        data (dict): The PROV-O compliant JSON-LD data structure representing the transaction's details.
        sender (str): Identifier of the agent initiating or primarily responsible for the transaction.
                      Derived from `agent_ids`.
        recipient (str): Identifier of the primary recipient or target of the transaction.
                         Derived from `agent_ids` or set to a default like "Ledger".
    """
    def __init__(self, transaction_type: str, asset_id_value: str, agent_ids: dict,
                 additional_props: dict = None, custom_base_uri: str = "urn:example:",
                 timestamp: float = None, transaction_id: str = None):
        """
        Constructs a new Transaction, generating its PROV-O data payload.

        Args:
            transaction_type (str): Type of transaction (e.g., "asset_creation").
            asset_id_value (str): Core identifier for the asset.
            agent_ids (dict): Identifiers for agents involved (contents depend on `transaction_type`).
            additional_props (dict, optional): Additional properties for PROV-O generation. Defaults to None.
            custom_base_uri (str, optional): Base URI for PROV-O identifiers. Defaults to "urn:example:".
            timestamp (float, optional): Unix timestamp for the transaction. Defaults to `time.time()`.
            transaction_id (str, optional): Specific ID for the transaction. Defaults to a new UUID URN.

        Raises:
            ValueError: If `transaction_type` is unsupported or required `agent_ids` are missing.
        """
        self.transaction_id = transaction_id if transaction_id else f"urn:uuid:{uuid4()}"
        self.timestamp = timestamp if timestamp else time.time()

        self.transaction_type = transaction_type
        self.asset_id_value = asset_id_value
        self.agent_ids = agent_ids
        self.additional_props = additional_props if additional_props else {}
        self.custom_base_uri = custom_base_uri

        # The ISO timestamp for PROV-O data is derived from the transaction's Unix timestamp.
        iso_timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.timestamp))

        if self.transaction_type == "asset_creation":
            if "creator_agent_id" not in agent_ids:
                raise ValueError("Missing 'creator_agent_id' in agent_ids for asset_creation.")
            self.data = create_asset_creation_prov(
                asset_id=self.asset_id_value, # Changed from asset_id_value
                creator_id=agent_ids["creator_agent_id"], # Changed from creator_agent_id_value
                creation_time_iso=iso_timestamp, # Match parameter name in definition
                additional_asset_props=self.additional_props, # Match parameter name
                # custom_base_uri is not a direct param of create_asset_creation_prov, it's used to build URIs passed in
                # activity_id and activity_label use defaults in serializer if not passed
            )
            self.sender = agent_ids["creator_agent_id"]
            self.recipient = "Ledger"
        elif self.transaction_type == "asset_transfer":
            if "from_agent_id" not in agent_ids or "to_agent_id" not in agent_ids:
                raise ValueError("Missing 'from_agent_id' or 'to_agent_id' in agent_ids for asset_transfer.")
            self.data = create_asset_transfer_prov(
                asset_id=self.asset_id_value, # Changed from asset_id_value
                from_agent_id=agent_ids["from_agent_id"], # Changed from from_agent_id_value
                to_agent_id=agent_ids["to_agent_id"], # Changed from to_agent_id_value
                transfer_time_iso=iso_timestamp, # Match parameter name in definition
                # additional_props for transfer maps to activity, not asset here.
                # activity_id and activity_label use defaults in serializer if not passed
            )
            self.sender = agent_ids["from_agent_id"]
            self.recipient = agent_ids["to_agent_id"]
        elif self.transaction_type == "system_genesis":
             self.data = {"@context": {}, "@graph": [{"@id": f"{custom_base_uri}genesis_event", "message": "Genesis Transaction"}]}
             self.sender = agent_ids.get("creator_agent_id", "system")
             self.recipient = "Ledger"
        else:
            raise ValueError(f"Unsupported transaction_type: {self.transaction_type}")

    def to_dict(self) -> dict:
        """
        Serializes the transaction to a dictionary.
        The `data` field contains the full PROV-O JSON-LD structure.

        Returns:
            dict: A dictionary representation of the transaction.
        """
        return {
            'transaction_id': self.transaction_id,
            'timestamp': self.timestamp,
            'transaction_type': self.transaction_type,
            'asset_id_value': self.asset_id_value,
            'agent_ids': self.agent_ids,
            'data': self.data
        }

    def to_json(self) -> str:
        """
        Serializes the transaction to a JSON string.

        Returns:
            str: A JSON string representation of the transaction, indented for readability.
        """
        return json.dumps(self.to_dict(), sort_keys=True, indent=2)

    @classmethod
    def from_dict(cls, tx_data_dict: dict) -> 'Transaction':
        """
        Creates a Transaction object from a dictionary representation (e.g., from `to_dict()`).

        This method reconstructs the Transaction object's state, including its PROV-O data.
        It does not re-run the PROV-O generation logic from `provo_serializer` but assumes
        the `data` field in `tx_data_dict` already contains the correct PROV-O structure.

        Args:
            tx_data_dict (dict): A dictionary conforming to the output of `to_dict()`.

        Returns:
            Transaction: The reconstructed Transaction object.
        """
        # Create an empty instance to avoid re-running __init__'s PROV-O generation.
        instance = cls.__new__(cls)

        instance.transaction_id = tx_data_dict['transaction_id']
        instance.timestamp = tx_data_dict['timestamp']
        instance.transaction_type = tx_data_dict['transaction_type']
        instance.asset_id_value = tx_data_dict.get('asset_id_value')
        instance.agent_ids = tx_data_dict.get('agent_ids', {})
        instance.data = tx_data_dict['data'] # Assumes 'data' is the PROV-O JSON-LD dict

        # Reconstruct sender/recipient based on stored agent_ids and type
        if instance.transaction_type == "asset_creation" and "creator_agent_id" in instance.agent_ids:
            instance.sender = instance.agent_ids["creator_agent_id"]
            instance.recipient = "Ledger"
        elif instance.transaction_type == "asset_transfer" and \
             "from_agent_id" in instance.agent_ids and "to_agent_id" in instance.agent_ids:
            instance.sender = instance.agent_ids["from_agent_id"]
            instance.recipient = instance.agent_ids["to_agent_id"]
        elif instance.transaction_type == "system_genesis":
            instance.sender = instance.agent_ids.get("creator_agent_id", "system")
            instance.recipient = "Ledger"
        else:
            # Fallback for potentially older formats or different types not fully handled here
            instance.sender = tx_data_dict.get('sender', None)
            instance.recipient = tx_data_dict.get('recipient', None)

        # Restore other relevant attributes used by __init__ if they were stored by to_dict()
        instance.additional_props = tx_data_dict.get('additional_props', {})
        instance.custom_base_uri = tx_data_dict.get('custom_base_uri', "urn:example:")

        return instance

    @classmethod
    def from_json(cls, tx_data_json: str) -> 'Transaction':
        """
        Creates a Transaction object from a JSON string representation.

        Args:
            tx_data_json (str): A JSON string, typically from `to_json()`.

        Returns:
            Transaction: The reconstructed Transaction object.
        """
        tx_data_dict = json.loads(tx_data_json)
        return cls.from_dict(tx_data_dict)

    def __repr__(self) -> str:
        """
        Returns a concise string representation of the Transaction object.
        """
        # Hash of data can be large; consider showing only a part or its type.
        data_preview = type(self.data).__name__
        if isinstance(self.data, dict) and "@graph" in self.data:
            data_preview = f"PROV-O Graph ({len(self.data['@graph'])} nodes)"

        return (f"Transaction(ID: {self.transaction_id[:13]}..., Type: {self.transaction_type}, "
                f"AssetVal: {self.asset_id_value}, Agents: {self.agent_ids}, Data: {data_preview})")


if __name__ == '__main__':
    print("--- Transaction with PROV-O Asset Creation ---")
    tx_create = Transaction(
        transaction_type="asset_creation",
        asset_id_value="SN999001",
        agent_ids={"creator_agent_id": "FactoryA"},
        additional_props={"ex:quality": "prime", "prov:atLocation": "Zone1"},
        custom_base_uri="urn:mycompany:"
    )
    print(tx_create)
    # print(tx_create.to_json())

    tx_create_dict = tx_create.to_dict()
    print("\nTo Dict (Creation):")
    # print(json.dumps(tx_create_dict, indent=2)) # Verbose
    assert tx_create_dict['data']['@graph'][0]['@id'] == "urn:mycompany:asset:SN999001"
    assert tx_create_dict['data']['@graph'][2]['@id'] == "urn:mycompany:agent:FactoryA"


    print("\n--- Transaction with PROV-O Asset Transfer ---")
    tx_transfer = Transaction(
        transaction_type="asset_transfer",
        asset_id_value="SN999001",
        agent_ids={"from_agent_id": "FactoryA", "to_agent_id": "DistributorX"},
        additional_props={"ex:shipment_id": "SHP0050"},
        custom_base_uri="urn:mycompany:"
    )
    print(tx_transfer)

    tx_transfer_dict = tx_transfer.to_dict()
    print("\nTo Dict (Transfer - checking one agent):")
    # print(json.dumps(tx_transfer_dict, indent=2)) # Verbose
    assert any(agent['@id'] == "urn:mycompany:agent:FactoryA" for agent in tx_transfer_dict['data']['@graph'])


    print("\n--- Test from_dict reconstruction ---")
    reconstructed_tx_create = Transaction.from_dict(tx_create_dict)
    print(f"Reconstructed Creation TX ID: {reconstructed_tx_create.transaction_id}")
    assert reconstructed_tx_create.transaction_id == tx_create.transaction_id
    assert reconstructed_tx_create.data == tx_create.data
    assert reconstructed_tx_create.sender == tx_create.sender

    reconstructed_tx_transfer = Transaction.from_json(tx_transfer.to_json())
    print(f"Reconstructed Transfer TX ID: {reconstructed_tx_transfer.transaction_id}")
    assert reconstructed_tx_transfer.transaction_id == tx_transfer.transaction_id
    assert reconstructed_tx_transfer.data == tx_transfer.data
    assert reconstructed_tx_transfer.sender == tx_transfer.sender
    assert reconstructed_tx_transfer.recipient == tx_transfer.recipient

    print("\nAll Transaction PROV-O integration __main__ tests passed.")
