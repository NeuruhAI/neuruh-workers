#!/usr/bin/env python3
"""
Worker 2 — Build Status Updater
Updates build status + appends dated build log to IP Registry entry.

Usage:
  python worker_2_build_status.py --asset-id AI-CB-002 --status "Built" --log "Phase 1 complete" --files "src/orchestration/Coordinator.ts"
  python worker_2_build_status.py --test
"""

import os, sys, json, argparse
from datetime import datetime

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError: pass

import requests

NOTION_API_KEY = os.environ.get("NOTION_API_KEY","")
IP_REGISTRY_DB = os.environ.get("IP_REGISTRY_DB_ID","3c48ca3132574937a3f3d729a93eb578")
NOTION_BASE = "https://api.notion.com/v1"
HEADERS = {"Authorization": f"Bearer {NOTION_API_KEY}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}

def find_by_asset_id(asset_id):
    resp = requests.post(f"{NOTION_BASE}/databases/{IP_REGISTRY_DB}/query", headers=HEADERS,
        json={"filter":{"property":"Asset ID","rich_text":{"equals":asset_id}},"page_size":1})
    results = resp.json().get("results",[]) if resp.status_code==200 else []
    return results[0] if results else None

def update_status(page_id, status, notes=""):
    props = {"Status":{"select":{"name":status}}}
    if notes: props["Notes"] = {"rich_text":[{"text":{"content":notes[:2000]}}]}
    resp = requests.patch(f"{NOTION_BASE}/pages/{page_id}", headers=HEADERS, json={"properties":props})
    return {"ok":resp.status_code==200}

def append_log(page_id, log_entry, files="", blocked_by=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    blocks = [
        {"object":"block","type":"heading_3","heading_3":{"rich_text":[{"text":{"content":f"Build Log — {now}"}}]}},
        {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"text":{"content":log_entry}}]}},
    ]
    if files: blocks.append({"object":"block","type":"paragraph","paragraph":{"rich_text":[{"text":{"content":f"Files: {files}"},"annotations":{"code":True}}]}})
    if blocked_by: blocks.append({"object":"block","type":"paragraph","paragraph":{"rich_text":[{"text":{"content":f"BLOCKED BY: {blocked_by}"},"annotations":{"bold":True,"color":"red"}}]}})
    blocks.append({"object":"block","type":"divider","divider":{}})
    resp = requests.patch(f"{NOTION_BASE}/blocks/{page_id}/children", headers=HEADERS, json={"children":blocks})
    return {"ok":resp.status_code==200}

def update_build(asset_id, status, log_entry="", files="", blocked_by=""):
    page = find_by_asset_id(asset_id)
    if not page: return {"ok":False,"error":f"Asset ID '{asset_id}' not found"}
    page_id = page["id"]
    notes = f"{status} — {log_entry}" if log_entry else status
    if blocked_by: notes += f" | BLOCKED: {blocked_by}"
    p = update_status(page_id, status, notes)
    l = append_log(page_id, log_entry, files, blocked_by) if log_entry else {"ok":True}
    return {"ok":p["ok"] and l["ok"],"page_id":page_id,"url":page.get("url","")}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset-id"); parser.add_argument("--status")
    parser.add_argument("--log", default=""); parser.add_argument("--files", default=""); parser.add_argument("--blocked-by", default="")
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    if not NOTION_API_KEY: print("[ERROR] NOTION_API_KEY not set", file=sys.stderr); sys.exit(1)
    if args.test:
        print(json.dumps(update_build("TEST-WORKER1","In Progress",f"Test at {datetime.now().isoformat()}"), indent=2)); return
    if not args.asset_id or not args.status: parser.error("--asset-id and --status required")
    print(json.dumps(update_build(args.asset_id, args.status, args.log, args.files, args.blocked_by), indent=2))

if __name__ == "__main__":
    main()
