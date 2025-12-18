import os
import requests
import sys

GITHUB_REPO = "microsoftgraph/microsoft-graph-docs"
SEARCH_URL = "https://api.github.com/search/code"
CONTENTS_URL_TPL = "https://api.github.com/repos/{owner}/{repo}/contents/{path}"

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
headers = {"Accept": "application/vnd.github.v3+json"}
if GITHUB_TOKEN:
    headers["Authorization"] = f"token {GITHUB_TOKEN}"

queries = [
    "permissions repo:microsoftgraph/microsoft-graph-docs",
    "permissions- repo:microsoftgraph/microsoft-graph-docs",
    "filename:permissions repo:microsoftgraph/microsoft-graph-docs",
    "filename:permissions-reference repo:microsoftgraph/microsoft-graph-docs",
]

seen = {}

for q in queries:
    print(f"\nSearching GitHub: {q}")
    params = {"q": q, "per_page": 100}
    try:
        r = requests.get(SEARCH_URL, headers=headers, params=params, timeout=20)
    except Exception as e:
        print(f"Request failed: {e}")
        continue
    if r.status_code == 403:
        print("Rate limited or forbidden. Provide a GITHUB_TOKEN for higher quota and access.")
        break
    if r.status_code != 200:
        print(f"Search failed: {r.status_code} {r.text[:200]}")
        continue
    data = r.json()
    total = data.get("total_count", 0)
    print(f"Found {total} results (showing up to 100)")
    for item in data.get("items", []):
        repo = item.get("repository", {})
        owner = repo.get("owner", {}).get("login", "microsoftgraph")
        repo_name = repo.get("name", "microsoft-graph-docs")
        path = item.get("path")
        key = f"{owner}/{repo_name}:{path}"
        if key in seen:
            continue
        seen[key] = True
        print(f"- {key}")
        # Try to get download_url via contents API
        contents_url = CONTENTS_URL_TPL.format(owner=owner, repo=repo_name, path=path)
        try:
            cr = requests.get(contents_url, headers=headers, params={"ref": repo.get("default_branch")}, timeout=10)
            if cr.status_code == 200:
                cdata = cr.json()
                download_url = cdata.get("download_url")
                print(f"  path: {path}")
                print(f"  repo default branch: {repo.get('default_branch')}")
                print(f"  download_url: {download_url}")
            else:
                print(f"  Could not fetch contents metadata: {cr.status_code}")
        except Exception as e:
            print(f"  Contents request failed: {e}")

print(f"\nTotal unique matches: {len(seen)}")
if len(seen) == 0:
    print("No matches found. We may need broader queries or manual inspection.")
