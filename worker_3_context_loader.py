#!/usr/bin/env python3
"""
Worker 3 — Session Context Loader
Run at session start. Queries IP Registry for active/blocked/in-progress work.

Usage:
  python worker_3_context_loader.py            # 5-line brief
  python worker_3_context_loader.py --full     # All active items
  python worker_3_context_loader.py --json     # JSON output
"""

import os, sys, json, argparse
from datetime import datetime
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests

NOTION_API_KEY = os.environ.get("NOTION_API_KEY", "")
IP_REGISTRY_DB = os.environ.get("IP_REGISTRY_DB_ID", "3c48ca3132574937a3f3d729a93eb578")
NOTION_BASE = "https://api.notion.com/v1"
HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def query_database(db_id, filter_obj=None, page_size=50):
    body = {"page_size": page_size}
    if filter_obj:
        body["filter"] = filter_obj
    resp = requests.post(f"{NOTION_BASE}/databases/{db_id}/query", headers=HEADERS, json=body)
    if resp.status_code != 200:
        print(f"[ERROR] {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
        return []
    return resp.json().get("results", [])

def extract_property(page, prop_name):
    prop = page.get("properties", {}).get(prop_name, {})
    t = prop.get("type", "")
    if t == "title": return "".join(p.get("plain_text","") for p in prop.get("title",[]))
    if t == "rich_text": return "".join(p.get("plain_text","") for p in prop.get("rich_text",[]))
    if t == "select": return (prop.get("select") or {}).get("name","")
    if t == "url": return prop.get("url","") or ""
    return ""

def load_ip_context(vertical=None, status_filter=None):
    statuses = [status_filter] if status_filter else ["In Progress", "Blocked", "Needs Review"]
    f = {"or": [{"property": "Status", "select": {"equals": s}} for s in statuses]}
    if vertical:
        f = {"and": [f, {"property": "Vertical", "select": {"equals": vertical}}]}
    pages = query_database(IP_REGISTRY_DB, f, 25)
    items = [{"title": extract_property(p,"IP Title") or extract_property(p,"Name"),
              "asset_id": extract_property(p,"Asset ID"), "status": extract_property(p,"Status"),
              "vertical": extract_property(p,"Vertical"), "notes": extract_property(p,"Notes")[:100],
              "url": p.get("url","")} for p in pages]
    order = {"In Progress":0,"Blocked":1,"Needs Review":2}
    return sorted(items, key=lambda x: order.get(x["status"],99))

def generate_brief(items):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    active = next((i for i in items if i["status"]=="In Progress"), None)
    active_str = f"{active['title']}" if active else "None — pick T1 build"
    blocked = [i for i in items if i["status"]=="Blocked"]
    blocked_str = ", ".join(b["title"] for b in blocked[:3]) if blocked else "None"
    return f"""=== NEURUH SESSION CONTEXT — {now} ===
ACTIVE BUILD: {active_str}
BLOCKERS: {blocked_str}
QUEUE: {len([i for i in items if i['status']=='In Progress'])} in-progress, {len(blocked)} blocked, {len([i for i in items if i['status']=='Needs Review'])} needs review
NEXT: {active['notes'][:80] if active and active['notes'] else 'Open HQ and pick highest-priority item'}"""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vertical")
    parser.add_argument("--status")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    if not NOTION_API_KEY:
        print("[ERROR] NOTION_API_KEY not set.", file=sys.stderr); sys.exit(1)

    items = load_ip_context(args.vertical, args.status)

    if args.json:
        print(json.dumps({"timestamp": datetime.now().isoformat(), "items": items}, indent=2))
    elif args.full:
        print(generate_brief(items))
        print("\n--- ALL ACTIVE ITEMS ---")
        for i in items:
            print(f"  [{i['status']}] {i['title']} ({i['asset_id']})")
    else:
        print(generate_brief(items))

if __name__ == "__main__":
    main()
