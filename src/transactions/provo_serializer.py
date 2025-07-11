import json
import time
from uuid import uuid4

# PROV-O namespace
PROV_O_CONTEXT_URL = "https://www.w3.org/ns/prov#"
# Example base URI for our own entities, activities, agents if not using full URLs
# For simplicity, we might use URNs or rely on context to expand simple IDs.
# EXAMPLE_BASE_URI = "http://example.com/ns/traceability#"

def get_provo_context():
    """Returns the JSON-LD context for PROV-O."""
    return {
        "@context": {
            "prov": PROV_O_CONTEXT_URL,
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            # Define common prefixes if needed, e.g.,
            # "ex": EXAMPLE_BASE_URI,
            # Define terms for our specific attributes if they don't map directly to prov terms
            # For example, if we have an "asset_type":
            # "assetType": "ex:assetType"

            # Standard PROV-O terms that we will use directly:
            "Entity": "prov:Entity",
            "Activity": "prov:Activity",
            "Agent": "prov:Agent",
            "wasGeneratedBy": {"@id": "prov:wasGeneratedBy", "@type": "@id"},
            "used": {"@id": "prov:used", "@type": "@id"},
            "wasAssociatedWith": {"@id": "prov:wasAssociatedWith", "@type": "@id"},
            "wasAttributedTo": {"@id": "prov:wasAttributedTo", "@type": "@id"},
            "startedAtTime": {"@id": "prov:startedAtTime", "@type": "xsd:dateTime"},
            "endedAtTime": {"@id": "prov:endedAtTime", "@type": "xsd:dateTime"},
            "atLocation": "prov:atLocation", # Example, could be string or structured
            "label": "rdfs:label" # from RDFS, often used for human-readable names
        }
    }

def create_asset_creation_prov(
    asset_id: str,
    creator_id: str,
    activity_id: str = None,
    creation_time_iso: str = None,
    additional_asset_props: dict = None,
    activity_label: str = "Asset Creation"
    ) -> dict:
    """
    Generates PROV-O JSON-LD for the creation of an asset.

    Args:
        asset_id (str): Unique identifier for the asset (e.g., "asset:123").
        creator_id (str): Unique identifier for the agent creating the asset (e.g., "agent:Alice").
        activity_id (str, optional): Unique ID for the creation activity. Defaults to a new UUID URN.
        creation_time_iso (str, optional): ISO 8601 timestamp for when the activity ended (asset generated). Defaults to now.
        additional_asset_props (dict, optional): Other properties to add to the asset entity (e.g., {"assetType": "Sensor"}).
        activity_label (str, optional): Human-readable label for the activity.

    Returns:
        dict: A JSON-LD object representing the provenance graph.
    """
    if not activity_id:
        activity_id = f"urn:uuid:{uuid4()}"
    if not creation_time_iso:
        creation_time_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    graph = []

    # Asset Entity
    asset_entity = {
        "@id": asset_id,
        "@type": "prov:Entity",
        "prov:wasGeneratedBy": activity_id,
        "prov:wasAttributedTo": creator_id,
        "rdfs:label": f"Asset {asset_id}"
    }
    if additional_asset_props:
        for key, value in additional_asset_props.items():
            # Ensure keys are valid for JSON-LD (e.g., prefixed or defined in context)
            asset_entity[key] = value
    graph.append(asset_entity)

    # Creation Activity
    activity = {
        "@id": activity_id,
        "@type": "prov:Activity",
        "rdfs:label": activity_label,
        "prov:endedAtTime": {"@type": "xsd:dateTime", "@value": creation_time_iso},
        # Optionally, if the activity used some input (e.g., raw materials)
        # "prov:used": "input_entity_id"
    }
    graph.append(activity)

    # Creator Agent
    agent = {
        "@id": creator_id,
        "@type": "prov:Agent",
        "rdfs:label": f"Agent {creator_id}"
    }
    # Add agent only if not already implicitly defined by usage
    # For a simple graph, it's good to explicitly define all nodes.
    graph.append(agent)

    # Link activity to agent
    # This is implicitly covered by wasAttributedTo on the entity if agent is creator,
    # but can be made explicit for the activity itself.
    activity_association = {
        "@id": activity_id, # Subject is the activity
        "prov:wasAssociatedWith": creator_id # Object is the agent
    }
    # Instead of appending another dict for activity_association, merge into activity:
    graph[graph.index(activity)]["prov:wasAssociatedWith"] = creator_id


    # Construct the full JSON-LD document
    # Using an explicit @graph array for multiple top-level descriptions
    json_ld_doc = {
        "@context": get_provo_context()["@context"], # Get just the context dict
        "@graph": graph
    }

    return json_ld_doc


def create_asset_transfer_prov(
    asset_id: str,
    from_agent_id: str,
    to_agent_id: str,
    activity_id: str = None,
    transfer_time_iso: str = None,
    activity_label: str = "Asset Transfer"
    ) -> dict:
    """
    Generates PROV-O JSON-LD for the transfer of an asset.
    This is a simplified model: shows the transfer activity, associated agents, and the asset involved.
    More complex models could show derivation of new ownership states.
    """
    if not activity_id:
        activity_id = f"urn:uuid:{uuid4()}"
    if not transfer_time_iso:
        transfer_time_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    graph = []

    # Asset Entity (referenced)
    asset_entity_ref = {"@id": asset_id, "@type": "prov:Entity", "rdfs:label": f"Asset {asset_id}"}
    graph.append(asset_entity_ref) # Define the entity being transferred

    # Transfer Activity
    activity = {
        "@id": activity_id,
        "@type": "prov:Activity",
        "rdfs:label": activity_label,
        "prov:used": asset_id, # The activity used/involved the asset
        "prov:endedAtTime": {"@type": "xsd:dateTime", "@value": transfer_time_iso},
        # Association with both agents involved in the transfer
        "prov:wasAssociatedWith": [from_agent_id, to_agent_id]
    }
    graph.append(activity)

    # From Agent
    from_agent = {"@id": from_agent_id, "@type": "prov:Agent", "rdfs:label": f"Agent {from_agent_id}"}
    graph.append(from_agent)

    # To Agent
    to_agent = {"@id": to_agent_id, "@type": "prov:Agent", "rdfs:label": f"Agent {to_agent_id}"}
    graph.append(to_agent)

    # Optionally, model new ownership:
    # A new Entity representing "Asset X under ownership of Agent Y" could be generated.
    # For simplicity here, we just record the transfer activity.
    # Example: Asset asset_id now wasAttributedTo to_agent_id AFTER this activity.
    # This could be represented by a new entity state or by updating the asset entity (if mutable).
    # For immutable ledger, usually new states are new entities derived from old.

    json_ld_doc = {
        "@context": get_provo_context()["@context"],
        "@graph": graph
    }
    return json_ld_doc


if __name__ == '__main__':
    print("--- Asset Creation PROV-O Example ---")
    asset_creation_data = create_asset_creation_prov(
        asset_id="urn:example:asset:widget001",
        creator_id="urn:example:user:Alice",
        additional_asset_props={"ex:color": "blue", "ex:weight_kg": 1.5},
        activity_label="Initial Manufacturing of Widget 001"
    )
    print(json.dumps(asset_creation_data, indent=2))

    print("\n--- Asset Transfer PROV-O Example ---")
    asset_transfer_data = create_asset_transfer_prov(
        asset_id="urn:example:asset:widget001",
        from_agent_id="urn:example:user:Alice",
        to_agent_id="urn:example:user:Bob",
        activity_label="Transfer of Widget 001 from Alice to Bob"
    )
    print(json.dumps(asset_transfer_data, indent=2))

    # Example of using pyld (if installed and needed for expansion/compacting)
    try:
        from pyld import jsonld
        expanded = jsonld.expand(asset_creation_data)
        print("\n--- Expanded JSON-LD (Asset Creation) ---")
        print(json.dumps(expanded, indent=2))
    except ImportError:
        print("\nNote: pyld library not installed. Skipping expansion example.")
    except Exception as e:
        print(f"\nError during JSON-LD expansion: {e}")
