"""
Worker 2 — Build Status Updater
Flips Build State on an IP Registry entry by Asset ID.

Usage:
    python3 worker_2_build_status.py AI-FW-017 "In Progress"

Valid states: Not Started, In Progress, Built, Blocked, Needs Review
"""
import os
import sys
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()
notion = Client(auth=os.environ["NOTION_API_KEY"])
DB = os.environ["IP_REGISTRY_DB_ID"]

VALID = ["Not Started", "In Progress", "Built", "Blocked", "Needs Review"]


def update(asset_id: str, new_state: str) -> None:
    if new_state not in VALID:
        raise ValueError(f"Invalid state '{new_state}'. Valid: {VALID}")

    q = notion.databases.query(
        database_id=DB,
        filter={"property": "Asset ID", "rich_text": {"equals": asset_id}},
    )
    if not q["results"]:
        print(f"No asset found with ID: {asset_id}")
        return

    page_id = q["results"][0]["id"]
    notion.pages.update(
        page_id=page_id,
        properties={"Build State": {"select": {"name": new_state}}},
    )
    print(f"{asset_id} -> {new_state}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 worker_2_build_status.py <asset_id> <new_state>")
        print(f"  Valid states: {VALID}")
        sys.exit(1)
    update(sys.argv[1], sys.argv[2])
