import os
import json
from datetime import datetime
import subprocess
from src.transactions.transaction import Transaction
from src.transactions.provo_serializer import create_asset_creation_prov, create_asset_transfer_prov

def benchmark_uht_milk_supply_chain(output_dir="results"):
    # Participants
    farm_id = "urn:example:agent:farm001"
    manufacturer_id = "urn:example:agent:manufacturer001"
    logistics_id = "urn:example:agent:logistics001"
    retail_id = "urn:example:agent:retail001"

    # Asset IDs
    raw_milk_id = "urn:example:asset:milkbatch001"
    uht_milk_id = "urn:example:asset:uhtbatch001"

    # Transaction sequence
    transactions = []
    transactions.append(Transaction(
        transaction_type="asset_creation",
        asset_id_value=raw_milk_id,
        agent_ids={"creator_agent_id": farm_id},
        additional_props={"ex:assetType": "RawMilk", "ex:volume_liters": 1000, "prov:atLocation": "Farm 001"}
    ))
    transactions.append(Transaction(
        transaction_type="asset_transfer",
        asset_id_value=raw_milk_id,
        agent_ids={"from_agent_id": farm_id, "to_agent_id": manufacturer_id},
        additional_props={"ex:shipment_id": "SHIP001"}
    ))
    transactions.append(Transaction(
        transaction_type="asset_creation",
        asset_id_value=uht_milk_id,
        agent_ids={"creator_agent_id": manufacturer_id},
        additional_props={"ex:assetType": "UHTMilk", "ex:volume_liters": 950, "prov:atLocation": "Manufacturer 001", "ex:sourceAsset": raw_milk_id}
    ))
    transactions.append(Transaction(
        transaction_type="asset_transfer",
        asset_id_value=uht_milk_id,
        agent_ids={"from_agent_id": manufacturer_id, "to_agent_id": logistics_id},
        additional_props={"ex:shipment_id": "SHIP002"}
    ))
    transactions.append(Transaction(
        transaction_type="asset_transfer",
        asset_id_value=uht_milk_id,
        agent_ids={"from_agent_id": logistics_id, "to_agent_id": retail_id},
        additional_props={"ex:shipment_id": "SHIP003"}
    ))

    # Prepare output file name with datetime
    os.makedirs(output_dir, exist_ok=True)
    dt_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"uht_milk_benchmark_{dt_str}.json")

    # Serialize all transactions
    tx_dicts = [tx.to_dict() for tx in transactions]
    with open(output_path, "w") as f:
        json.dump(tx_dicts, f, indent=2)
    print(f"Benchmark transactions written to: {output_path}")


def run_all_consensus_benchmarks():
    consensus_protocols = ["poa", "pbft", "hotstuff", "pos", "dpos", "pow"]
    num_nodes = 4
    num_transactions = 5
    tx_rate = 2
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True)

    dt_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    for protocol in consensus_protocols:
        output_file = os.path.join(output_dir, f"benchmark_{protocol}_{dt_str}.json")
        print(f"Running benchmark for {protocol.upper()}...")
        cmd = [
            "python3", "run_benchmark.py",
            "--consensus", protocol,
            "--num_nodes", str(num_nodes),
            "--num_transactions", str(num_transactions),
            "--tx_rate", str(tx_rate),
            "--output", output_file
        ]
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error running benchmark for {protocol}: {e.stderr}")

    print("\nAll consensus protocol benchmarks completed.")


if __name__ == "__main__":
    benchmark_uht_milk_supply_chain()
    run_all_consensus_benchmarks()
