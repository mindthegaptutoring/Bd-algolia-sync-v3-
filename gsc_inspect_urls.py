"""Bulk index check via the Search Console URL Inspection API.
Reads zero_impression_listings_to_check.csv (needs a 'url' column) and writes
index_check_results.csv. Uses the same service account as listing_stats_sync.py.

Env vars (same names as the sync script):
  GOOGLE_CREDENTIALS_PATH  default /etc/secrets/google-credentials.json
  GSC_SITE_URL             default https://www.learnwitheveryavenue.com/
Usage: python gsc_inspect_urls.py [input.csv] [output.csv]
"""
import csv, os, sys, time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

CREDS = os.environ.get("GOOGLE_CREDENTIALS_PATH", "/etc/secrets/google-credentials.json")
SITE = os.environ.get("GSC_SITE_URL", "https://www.learnwitheveryavenue.com/")
src = sys.argv[1] if len(sys.argv) > 1 else "zero_impression_listings_to_check.csv"
dst = sys.argv[2] if len(sys.argv) > 2 else "index_check_results.csv"

creds = service_account.Credentials.from_service_account_file(
    CREDS, scopes=["https://www.googleapis.com/auth/webmasters.readonly"])
svc = build("searchconsole", "v1", credentials=creds)

def inspect(url):
    for attempt in range(3):
        try:
            r = svc.urlInspection().index().inspect(
                body={"inspectionUrl": url, "siteUrl": SITE}).execute()
            return r.get("inspectionResult", {}).get("indexStatusResult", {})
        except HttpError as e:
            if e.resp.status in (429, 500, 503):
                time.sleep(5 * (attempt + 1)); continue
            return {"verdict": "ERROR", "coverageState": str(e)[:200]}
    return {"verdict": "ERROR", "coverageState": "retries exhausted"}

rows = list(csv.DictReader(open(src, newline="", encoding="utf-8")))
out = []
for i, row in enumerate(rows, 1):
    s = inspect(row["url"])
    declared, google = s.get("userCanonical", ""), s.get("googleCanonical", "")
    out.append({
        "educator": row.get("educator", ""), "title": row.get("title", ""),
        "url": row["url"],
        "verdict": s.get("verdict", ""),          # PASS = indexed
        "coverage_state": s.get("coverageState", ""),
        "last_crawl": s.get("lastCrawlTime", ""),
        "robots": s.get("robotsTxtState", ""),
        "indexing_allowed": s.get("indexingState", ""),
        "declared_canonical": declared, "google_canonical": google,
        "canonical_mismatch": "YES" if google and declared and google != declared else "",
    })
    print(f"{i}/{len(rows)} {s.get('verdict','?')}: {s.get('coverageState','')}  {row['url'][-60:]}")
    time.sleep(0.5)  # well under the 600/min limit

with open(dst, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(out[0].keys()))
    w.writeheader(); w.writerows(out)
print("wrote", dst)
