#!/usr/bin/env python3
"""Secret scan over tracked files.  Exit 0 = clean, exit 1 = findings.

Patterns cover the credential shapes this workspace has actually had to
contain: Notion secrets, API keys, cloud keys, private key blocks, bearer
tokens.  Placeholders (REPLACE_ME) are structurally unmatchable.
"""
from __future__ import annotations

import re
import subprocess
import sys

PATTERNS = [
    ("notion_secret", re.compile(r"secret_[A-Za-z0-9]{20,}")),
    ("openai_style_key", re.compile(r"\bsk-[A-Za-z0-9\-_]{20,}")),
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("private_key_block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("github_token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}")),
    ("slack_token", re.compile(r"\bxox[abpsr]-[A-Za-z0-9\-]{10,}")),
    ("notion_internal_token", re.compile(r"\bntn_[A-Za-z0-9]{20,}")),
    ("openrouter_key", re.compile(r"\bsk-or-[A-Za-z0-9\-]{16,}")),
    ("bearer_literal", re.compile(r"Bearer [A-Za-z0-9\-_\.]{30,}")),
]


def tracked_files() -> list[str]:
    out = subprocess.run(["git", "ls-files"], capture_output=True, text=True,
                         check=True)
    return [f for f in out.stdout.splitlines() if f]


def scan() -> list[tuple[str, int, str]]:
    findings = []
    for path in tracked_files():
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                for lineno, line in enumerate(fh, start=1):
                    for label, pattern in PATTERNS:
                        if pattern.search(line):
                            findings.append((path, lineno, label))
        except (IsADirectoryError, FileNotFoundError):
            continue
    return findings


def main() -> int:
    findings = scan()
    if findings:
        print("SECRET SCAN: FINDINGS")
        for path, lineno, label in findings:
            print(f"  {path}:{lineno} — {label}")
        return 1
    print(f"SECRET SCAN: CLEAN ({len(tracked_files())} tracked files, "
          f"{len(PATTERNS)} patterns)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
