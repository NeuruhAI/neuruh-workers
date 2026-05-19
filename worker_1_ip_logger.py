#!/usr/bin/env python3
"""
Worker 1 — IP Registry Logger
Upserts assets into Neuruh IP Registry.

Usage:
  python worker_1_ip_logger.py --asset-id AI-CB-010 --title "Name" --vertical "AI Platform Core" --status "In Progress"
  python worker_1_ip_logger.py --test
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
        json={"filter": {"property":"Asset ID","rich_text":{"equals":asset_id}},"page_size":1})
    results = resp.json().get("results",[]) if resp.status_code==200 else []
    return results[0] if results else None

def create_entry(asset_id, title, vertical, status, github="", notes=""):
    props = {
        "IP Title": {"title":[{"text":{"content":title}}]},
        "Asset ID": {"rich_text":[{"text":{"content":asset_id}}]},
        "Status": {"select":{"name":status}},
    }
    if vertical: props["Vertical"] = {"select":{"name":vertical}}
    if github: props["GitHub Repo"] = {"url":github}
    if notes: props["Notes"] = {"rich_text":[{"text":{"content":notes}}]}
    resp = requests.post(f"{NOTION_BASE}/pages", headers=HEADERS,
        json={"parent":{"database_id":IP_REGISTRY_DB},"properties":props})
    if resp.status_code not in (200,201):
        return {"ok":False,"error":resp.text[:200]}
    return {"ok":True,"url":resp.json().get("url",""),"created":True}

def update_entry(page_id, updates):
    props = {}
    if "title" in updates: props["IP Title"] = {"title":[{"text":{"content":updates["title"]}}]}
    if "status" in updates: props["Status"] = {"select":{"name":updates["status"]}}
    if "vertical" in updates: props["Vertical"] = {"select":{"name":updates["vertical"]}}
    if "github" in updates: props["GitHub Repo"] = {"url":updates["github"]}
    if "notes" in updates: props["Notes"] = {"rich_text":[{"text":{"content":updates["notes"]}}]}
    resp = requests.patch(f"{NOTION_BASE}/pages/{page_id}", headers=HEADERS, json={"properties":props})
    return {"ok":resp.status_code==200,"url":resp.json().get("url","") if resp.status_code==200 else "","created":False}

def upsert(asset_id, title, vertical, status, github="", notes=""):
    existing = find_by_asset_id(asset_id)
    if existing:
        print(f"[Worker 1] Updating: {asset_id}")
        return update_entry(existing["id"], {"title":title,"status":status,"vertical":vertical,"github":github,"notes":notes})
    print(f"[Worker 1] Creating: {asset_id}")
    return create_entry(asset_id, title, vertical, status, github, notes)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset-id"); parser.add_argument("--title")
    parser.add_argument("--vertical", default=""); parser.add_argument("--status", default="Not Started")
    parser.add_argument("--github", default=""); parser.add_argument("--notes", default="")
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    if not NOTION_API_KEY: print("[ERROR] NOTION_API_KEY not set", file=sys.stderr); sys.exit(1)
    if args.test:
        result = upsert("TEST-WORKER1","Worker 1 Test Entry","AI Platform Core","Needs Review",notes=f"Test {datetime.now().isoformat()}")
        print(json.dumps(result, indent=2)); return
    if not args.asset_id or not args.title: parser.error("--asset-id and --title required")
    print(json.dumps(upsert(args.asset_id, args.title, args.vertical, args.status, args.github, args.notes), indent=2))

if __name__ == "__main__":
    main()
