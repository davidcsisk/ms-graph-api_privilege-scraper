import os
import requests

GITHUB_API_SEARCH = "https://api.github.com/search/code"
GITHUB_API_CONTENTS = "https://api.github.com/repos/{owner}/{repo}/contents/{path}"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
headers = {"Accept": "application/vnd.github.v3+json"}
if GITHUB_TOKEN:
    headers["Authorization"] = f"token {GITHUB_TOKEN}"

queries = [
    "org:microsoftgraph permission",
    "org:microsoftgraph filename:permissions extension:md",
    "org:microsoftgraph path:api-reference permission",
    "org:microsoftgraph path:api-reference filename:permission",
]

seen = {}

for q in queries:
    print(f"\nQuery: {q}")
    params = {"q": q, "per_page": 100}
    try:
        r = requests.get(GITHUB_API_SEARCH, headers=headers, params=params, timeout=30)
    except Exception as e:
        print(f"Search request failed: {e}")
        continue
    if r.status_code == 403:
        print("Rate limited or forbidden. Provide a GITHUB_TOKEN for higher quota.")
        break
    if r.status_code != 200:
        print(f"Search failed: {r.status_code} {r.text[:200]}")
        continue
    data = r.json()
    total = data.get("total_count", 0)
    print(f"Found {total} results (showing up to 100)")
    for item in data.get("items", []):
        repo = item.get("repository", {})
        owner = repo.get("owner", {}).get("login")
        repo_name = repo.get("name")
        path = item.get("path")
        key = f"{owner}/{repo_name}:{path}"
        if key in seen:
            continue
        seen[key] = True
        print(f"- {key}")
        try:
            cr = requests.get(GITHUB_API_CONTENTS.format(owner=owner, repo=repo_name, path=path), headers=headers, params={"ref": repo.get("default_branch")}, timeout=15)
            if cr.status_code == 200:
                cdata = cr.json()
                print(f"  download_url: {cdata.get('download_url')}")
            else:
                print(f"  contents fetch failed: {cr.status_code}")
        except Exception as e:
            print(f"  contents request failed: {e}")

print(f"\nTotal unique matches in org:microsoftgraph: {len(seen)}")
