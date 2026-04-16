"""
Worker 3 — Context Loader
Queries the IP Registry for active (In Progress / Blocked / Needs Review) work
and writes a clean session brief to ~/neuruh/session-context.md.

Alias (add to ~/.zshrc):
    alias ctx='cd ~/neuruh/workers && source .venv/bin/activate && python3 worker_3_context_loader.py && cat ~/neuruh/session-context.md'
"""
import os
import datetime
from pathlib import Path
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()
notion = Client(auth=os.environ["NOTION_API_KEY"])


def active_ip():
    q = notion.databases.query(
        database_id=os.environ["IP_REGISTRY_DB_ID"],
        filter={
            "or": [
                {"property": "Build State", "select": {"equals": "In Progress"}},
                {"property": "Build State", "select": {"equals": "Blocked"}},
                {"property": "Build State", "select": {"equals": "Needs Review"}},
            ]
        },
    )
    return q["results"]


def _first_text(rich):
    return rich[0]["plain_text"] if rich else "-"


def render() -> Path:
    out = [f"# Neuruh Session Context - {datetime.date.today()}", ""]
    out.append("## Active IP (In Progress / Blocked / Needs Review)")
    out.append("")

    rows = active_ip()
    if not rows:
        out.append("_(nothing active - pick your next build from the backlog)_")
    else:
        for r in rows:
            p = r["properties"]
            title = _first_text(p["IP Title"]["title"])
            aid = _first_text(p["Asset ID"]["rich_text"])
            state = p["Build State"]["select"]["name"] if p["Build State"]["select"] else "-"
            vertical = p["Vertical"]["select"]["name"] if p["Vertical"]["select"] else "-"
            desc = _first_text(p["Description"]["rich_text"])[:160]
            out.append(f"- **{aid}** - {title} -> *{state}* - `{vertical}`")
            if desc and desc != "-":
                out.append(f"    {desc}")

    path = Path.home() / "neuruh" / "session-context.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(out) + "\n")
    print(f"Context written to {path}")
    return path


if __name__ == "__main__":
    render()
