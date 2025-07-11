# PROV-O Serialization Notes

This document outlines how PROV-O (W3C Provenance Ontology) is used within this blockchain consensus benchmark project to structure transaction data. Transactions are serialized as JSON-LD.

## Core Concepts Used:

The implementation primarily uses the following PROV-O concepts:

-   **`prov:Entity`**: Represents the asset being transacted or tracked.
    -   Identified by a URI, typically constructed using a base URI and the asset's unique value (e.g., `urn:example:asset:SN123`).
-   **`prov:Activity`**: Represents the operation performed on or involving the asset, such as its creation or transfer.
    -   Each activity is given a unique URI (e.g., a URN like `urn:uuid:<generated_uuid>`).
    -   `prov:endedAtTime` is used to timestamp the completion of the activity, using ISO 8601 format.
-   **`prov:Agent`**: Represents the parties involved in the transaction (e.g., creator, sender, recipient).
    -   Identified by URIs (e.g., `urn:example:agent:Alice`).

## JSON-LD Context:

A common JSON-LD `@context` is defined in `src/transactions/provo_serializer.py` (variable `PROV_O_CONTEXT`). This context includes:
-   The standard PROV-O namespace: `prov: "http://www.w3.org/ns/prov#"`
-   XML Schema namespace for data types: `xsd: "http://www.w3.org/2001/XMLSchema#"`
-   RDFS namespace for terms like `rdfs:label`: `rdfs: "http://www.w3.org/2000/01/rdf-schema#"`
-   An example custom namespace: `ex: "http://example.com/ns/traceability#"` for domain-specific terms if needed.
-   Mappings for commonly used PROV-O terms (e.g., `Entity: "prov:Entity"`, `wasGeneratedBy: {"@id": "prov:wasGeneratedBy", "@type": "@id"}`). The `@type: "@id"` indicates that the value of this property is a URI.

## Transaction Type Specific Mappings:

The `src/transactions/provo_serializer.py` module provides functions to generate PROV-O data for specific transaction types:

### 1. Asset Creation (`create_provo_asset_creation`)

-   **Asset (Entity)**:
    -   `@id`: URI of the newly created asset.
    -   `@type`: `prov:Entity`.
    -   `rdfs:label`: Human-readable label (e.g., "Asset SN123").
    -   `prov:wasGeneratedBy`: Links to the creation `Activity`'s URI.
    -   `prov:wasAttributedTo`: Links to the creator `Agent`'s URI.
    -   Additional custom properties (e.g., `ex:color`, `prov:atLocation`) can be included.
-   **Creation (Activity)**:
    -   `@id`: Unique URI for the activity.
    -   `@type`: `prov:Activity`.
    -   `rdfs:label`: Human-readable label (e.g., "Creation of Asset SN123").
    -   `prov:endedAtTime`: Timestamp of creation.
    -   `prov:wasAssociatedWith`: Links to the creator `Agent`'s URI.
-   **Creator (Agent)**:
    -   `@id`: URI of the creator.
    -   `@type`: `prov:Agent`.
    -   `rdfs:label`: Human-readable label (e.g., "Agent Alice").

### 2. Asset Transfer (`create_provo_asset_transfer`)

-   **Asset (Entity Reference)**:
    -   `@id`: URI of the asset being transferred.
    -   `@type`: `prov:Entity`.
    -   `rdfs:label`: Human-readable label.
    -   *(Note: This is often a reference. The full asset definition might exist from its creation event.)*
-   **Transfer (Activity)**:
    -   `@id`: Unique URI for the transfer activity.
    -   `@type`: `prov:Activity`.
    -   `rdfs:label`: Human-readable label (e.g., "Transfer of Asset SN123 from Alice to Bob").
    -   `prov:used`: Links to the asset `Entity`'s URI.
    -   `prov:endedAtTime`: Timestamp of transfer.
    -   `prov:wasAssociatedWith`: An array containing URIs of both the `from_agent_id` and `to_agent_id`.
-   **From Agent (Agent)**:
    -   `@id`: URI of the agent transferring the asset.
    -   `@type`: `prov:Agent`.
    -   `rdfs:label`: Human-readable label.
-   **To Agent (Agent)**:
    -   `@id`: URI of the agent receiving the asset.
    -   `@type`: `prov:Agent`.
    -   `rdfs:label`: Human-readable label.

*(Note: A more advanced transfer model could generate a new `Entity` state representing the asset under new ownership, derived from its previous state using `prov:wasDerivedFrom`.)*

## Storage in Transaction:

The generated JSON-LD dictionary (including its `@context` and `@graph`) is stored in the `data` field of the `Transaction` object (see `src/transactions/transaction.py`). The `Transaction.to_dict()` method then includes this entire structure when serializing the transaction for inclusion in a block.
This allows the blockchain to store rich, standardized provenance records.
