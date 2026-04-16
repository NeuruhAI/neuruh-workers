"""
Worker 1 — IP Logger
Files candidate IP assets to the Neuruh IP Registry via the Notion API.
Auto-generates Asset IDs using VERTICAL-TYPE-### convention.

Usage:
    python3 worker_1_ip_logger.py "Title" AI FW "Description" [github_url] [drive_url]

Vertical codes: AI, RE, RSL, MKT, SLS, LEAD, DEF, MEDIA, FIN, OPS
Type codes:     FW, PR, CB, RS, AR, MTH
"""
import os
import sys
import datetime
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()
notion = Client(auth=os.environ["NOTION_API_KEY"])
DB = os.environ["IP_REGISTRY_DB_ID"]

VERTICAL_MAP = {
    "AI": "AI Platform Core",
    "RE": "Real Estate",
    "RSL": "Resale & Arbitrage",
    "MKT": "Marketing",
    "SLS": "Sales",
    "LEAD": "Lead Generation",
    "DEF": "Defense",
    "MEDIA": "Content & Media",
    "FIN": "Finance & Legal",
    "OPS": "Ops & Automation",
}
TYPE_MAP = {
    "FW": "Framework",
    "PR": "Protocol",
    "CB": "Codebase",
    "RS": "Research",
    "AR": "Article",
    "MTH": "Methodology",
}


def next_asset_id(vertical_code: str, type_code: str) -> str:
    """Query existing assets with the same prefix and return the next sequential ID."""
    prefix = f"{vertical_code}-{type_code}-"
    query = notion.databases.query(
        database_id=DB,
        filter={"property": "Asset ID", "rich_text": {"starts_with": prefix}},
    )
    n = len(query["results"]) + 1
    return f"{prefix}{n:03d}"


def file_asset(title, vertical_code, type_code, description, github="", drive=""):
    if vertical_code not in VERTICAL_MAP:
        raise ValueError(f"Unknown vertical '{vertical_code}'. Valid: {list(VERTICAL_MAP)}")
    if type_code not in TYPE_MAP:
        raise ValueError(f"Unknown type '{type_code}'. Valid: {list(TYPE_MAP)}")

    aid = next_asset_id(vertical_code, type_code)
    notion.pages.create(
        parent={"database_id": DB},
        properties={
            "IP Title": {"title": [{"text": {"content": title}}]},
            "Asset ID": {"rich_text": [{"text": {"content": aid}}]},
            "Vertical": {"select": {"name": VERTICAL_MAP[vertical_code]}},
            "Type": {"select": {"name": TYPE_MAP[type_code]}},
            "Build State": {"select": {"name": "Not Started"}},
            "Status": {"select": {"name": "Draft"}},
            "Resale Ready": {"select": {"name": "With Customization"}},
            "Description": {"rich_text": [{"text": {"content": description[:2000]}}]},
            "Date Filed": {"date": {"start": datetime.date.today().isoformat()}},
            "GitHub Repo": {"url": github or None},
            "Drive URL": {"url": drive or None},
        },
    )
    print(f"Filed {aid}: {title}")
    return aid


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python3 worker_1_ip_logger.py <title> <vertical> <type> <description> [github_url] [drive_url]")
        print(f"  Verticals: {list(VERTICAL_MAP)}")
        print(f"  Types:     {list(TYPE_MAP)}")
        sys.exit(1)

    title = sys.argv[1]
    vertical = sys.argv[2]
    type_code = sys.argv[3]
    description = sys.argv[4]
    github = sys.argv[5] if len(sys.argv) > 5 else ""
    drive = sys.argv[6] if len(sys.argv) > 6 else ""
    file_asset(title, vertical, type_code, description, github, drive)
