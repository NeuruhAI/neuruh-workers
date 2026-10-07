"""The repository must scan clean, and the scanner must actually detect."""
import os
import subprocess
import sys
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCANNER = os.path.join(REPO_ROOT, "scripts", "secret_scan.py")


class SecretScanTests(unittest.TestCase):
    def test_repository_is_clean(self):
        result = subprocess.run([sys.executable, SCANNER], cwd=REPO_ROOT,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("SECRET SCAN: CLEAN", result.stdout)

    def test_scanner_detects_planted_secret(self):
        sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))
        try:
            import secret_scan
        finally:
            sys.path.pop(0)
        line = "NOTION_API_KEY=secret_" + "a1B2c3D4e5" * 3
        hits = [label for label, pat in secret_scan.PATTERNS if pat.search(line)]
        self.assertIn("notion_secret", hits)
        key_line = "-----BEGIN RSA " + "PRIVATE KEY-----"  # split so this file scans clean
        hits = [label for label, pat in secret_scan.PATTERNS if pat.search(key_line)]
        self.assertIn("private_key_block", hits)


if __name__ == "__main__":
    unittest.main()
