import os
import requests
import re
import csv
from typing import List
from bs4 import BeautifulSoup

# Prefer listing files via the GitHub Contents API rather than constructing
# raw.githubusercontent URLs (which can change or 404).
GITHUB_REPO_CANDIDATES = [
    "microsoftgraph/microsoft-graph-docs",
    "microsoftgraph/microsoft-graph-docs-contrib",
]
# Try multiple include paths commonly used in the docs repos
INCLUDES_PATHS = [
    "api-reference/beta/includes",
    "api-reference/v1.0/includes",
    "api-reference/main/includes",
    "api-reference/includes",
]

# Optional token to increase rate limits
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# If API listing fails, fall back to these known filenames (legacy behavior)
PERMISSION_FILES = [
    "permissions-reference.md",
    "permissions-directory.md",
    "permissions-mail.md",
    "permissions-calendar.md",
    "permissions-device.md",
    "permissions-intune.md",
    "permissions-identity.md",
    "permissions-teamwork.md",
    "permissions-identity-governance.md",
    "permissions-cloudcommunications.md",
]


def list_include_files_for_repo(owner_repo: str, includes_path: str, headers: dict) -> List[dict]:
    url = f"https://api.github.com/repos/{owner_repo}/contents/{includes_path}"
    r = requests.get(url, headers=headers, params={"ref": "main"}, timeout=15)
    if r.status_code == 404:
        raise RuntimeError(f"Includes path not found in {owner_repo}/{includes_path} (HTTP 404)")
    r.raise_for_status()
    return r.json()

def guess_privilege_score(name: str):
    """Simple heuristic to assign privilege level 1–20."""
    name_lower = name.lower()
    if "read" in name_lower:
        return 5
    if "readwrite" in name_lower or "update" in name_lower:
        return 10
    if "delete" in name_lower or "write" in name_lower:
        return 15
    if "all" in name_lower or "full" in name_lower:
        return 20
    return 8

def extract_permissions_from_markdown(md_text):
    """
    Extracts permission rows from GitHub markdown.
    Markdown uses tables like:
    | Permission | Type | Description |
    """
    permissions = []
    seen = set()

    lines = md_text.splitlines()
    i = 0
    # Find markdown tables and parse them
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith('|') and i + 1 < len(lines) and re.search(r"^\|[:\-\s\|]+$", lines[i+1].strip()):
            # collect table block
            block = []
            j = i
            while j < len(lines) and lines[j].strip().startswith('|'):
                block.append(lines[j])
                j += 1

            header_cols = [c.strip() for c in block[0].split('|')[1:-1]]
            # determine columns
            header_lc = [h.lower() for h in header_cols]
            # find indices
            try:
                idx_type = next((k for k, h in enumerate(header_lc) if 'permission type' in h or h.strip().startswith('permission')))
            except StopIteration:
                idx_type = 0
            idx_least = next((k for k, h in enumerate(header_lc) if 'least' in h or 'least privileged' in h), None)
            idx_higher = next((k for k, h in enumerate(header_lc) if 'higher' in h or 'higher privileged' in h), None)
            idx_description = next((k for k, h in enumerate(header_lc) if 'description' in h or 'details' in h), None)

            for row in block[2:]:
                cols = [c.strip() for c in row.split('|')[1:-1]]
                if not cols:
                    continue
                # Get permission type text (Delegated/Application/etc.)
                perm_type_text = cols[idx_type] if idx_type < len(cols) else ''
                perm_kind = 'delegated' if 'delegated' in perm_type_text.lower() else ('application' if 'application' in perm_type_text.lower() else None)
                if perm_kind is None:
                    continue

                # collect permission names from least and higher columns
                candidates = []
                if idx_least is not None and idx_least < len(cols):
                    candidates.append(cols[idx_least])
                if idx_higher is not None and idx_higher < len(cols):
                    candidates.append(cols[idx_higher])

                # capture description cell if present for this row
                row_description = ''
                if idx_description is not None and idx_description < len(cols):
                    row_description = cols[idx_description]

                for cand in candidates:
                    # split comma-separated lists
                    parts = [p.strip() for p in re.split(r',|;|\\band\\b', cand) if p.strip()]
                    for p in parts:
                        # skip unsupported notes
                        low = p.lower()
                        if low.startswith('not supported') or low.startswith('not applicable') or low.startswith('n/a'):
                            continue
                        # permission tokens often look like Group.Read.All or User.ReadWrite.All
                        # extract tokens that look like permissions
                        tokens = re.findall(r"[A-Za-z0-9_\-]+(?:\.[A-Za-z0-9_\-]+)+", p)
                        for tok in tokens:
                            if tok in seen:
                                continue
                            seen.add(tok)
                            # prefer description from a Description column if available
                            desc = row_description if row_description and row_description.strip() else (cand if cand and cand.strip() else '')
                            permissions.append({
                                'privilege_type': 'scp' if perm_kind == 'delegated' else 'roles',
                                'privilege_name': tok,
                                'privilege_description': desc,
                                'privilege_score': guess_privilege_score(tok)
                            })

            i = j
            continue
        i += 1

    # Fallback: scan entire document for permission-like tokens (e.g., User.Read.All)
    tokens = re.findall(r"\b[A-Za-z0-9_\-]+(?:\.[A-Za-z0-9_\-]+)+\b", md_text)
    # Build a paragraph-level description map: any paragraph that contains a token
    # and has more content will be used as a candidate long description.
    desc_map = {}
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", md_text) if p.strip()]
    for p in paragraphs:
        found = re.findall(r"\b[A-Za-z0-9_\-]+(?:\.[A-Za-z0-9_\-]+)+\b", p)
        for f in found:
            # prefer longer paragraphs as descriptions
            if len(p) > len(f) + 20 and f not in desc_map:
                desc_map[f] = ' '.join([l.strip() for l in p.splitlines()])

    for tok in tokens:
        if tok in seen:
            continue
        low = tok.lower()
        if '.all' in low or 'read' in tok or 'write' in tok or 'permission' in low:
            seen.add(tok)
            # try to get a paragraph description
            para_desc = desc_map.get(tok, '')
            permissions.append({
                'privilege_type': 'scp',
                'privilege_name': tok,
                'privilege_description': para_desc,
                'privilege_score': guess_privilege_score(tok)
            })

    return permissions

all_permissions = []

# Try to list files via GitHub API and download file contents by download_url.
headers = {"Accept": "application/vnd.github.v3+json"}
if GITHUB_TOKEN:
    headers["Authorization"] = f"token {GITHUB_TOKEN}"

perm_files_to_download = []

# Recursively traverse a contents path and collect files matching 'permission' in the name
def collect_permission_files(repo: str, path: str):
    try:
        items = list_include_files_for_repo(repo, path, headers)
    except Exception:
        return []
    results = []
    for it in items:
        it_type = it.get("type")
        name = it.get("name", "")
        if it_type == "file":
            if "permission" in name.lower() and name.lower().endswith(".md"):
                results.append({"name": name, "download_url": it.get("download_url")})
        elif it_type == "dir":
            subpath = it.get("path")
            results.extend(collect_permission_files(repo, subpath))
    return results

# Try candidate repos and include paths; recurse into directories to find permissions files
found_any = False
for repo in GITHUB_REPO_CANDIDATES:
    for inc_path in INCLUDES_PATHS:
        matches = collect_permission_files(repo, inc_path)
        if matches:
            print(f"Found {len(matches)} permission files in {repo}/{inc_path}")
            perm_files_to_download.extend(matches)
            found_any = True
    if found_any:
        break

if not perm_files_to_download:
    print("Could not discover permission files via the GitHub API in candidate repos/paths.")
    print("Falling back to legacy raw URLs for known permission files...")
    base = "https://raw.githubusercontent.com/microsoftgraph/microsoft-graph-docs/main/api-reference/beta/includes/"
    for f in PERMISSION_FILES:
        perm_files_to_download.append({"name": f, "download_url": base + f})

download_dir = os.path.join(os.path.dirname(__file__), "downloaded_permissions")
os.makedirs(download_dir, exist_ok=True)

for it in perm_files_to_download:
    name = it.get("name")
    download_url = it.get("download_url")
    if not download_url:
        print(f"No download URL for {name}, skipping")
        continue
    print(f"Downloading {name} from {download_url}...")
    try:
        r = requests.get(download_url, timeout=20)
        if r.status_code == 200:
            # Save raw markdown for inspection
            safe_name = name.replace('/', '_')
            path = os.path.join(download_dir, safe_name)
            try:
                with open(path, 'w', encoding='utf-8') as fh:
                    fh.write(r.text)
            except Exception as e:
                print(f"Failed to save {safe_name}: {e}")

            perms = extract_permissions_from_markdown(r.text)
            if perms:
                print(f"Parsed {len(perms)} permissions from {name}")
                all_permissions.extend(perms)
            else:
                print(f"No permissions parsed from {name}")
        else:
            print(f"Failed to download {name}: {r.status_code}")
    except requests.RequestException as e:
        print(f"Failed to download {name}: {e}")

# Remove duplicates by permission name
unique = {p["privilege_name"]: p for p in all_permissions}
final_list = list(unique.values())

# Write CSV to the scraper script directory by default
output = os.path.join(os.path.dirname(__file__), "graph_permissions.csv")
os.makedirs(os.path.dirname(output), exist_ok=True)
with open(output, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "privilege_type",
            "privilege_name",
            "privilege_description",
            "privilege_score"
        ]
    )
    writer.writeheader()
    writer.writerows(final_list)

print(f"\nCSV generated: {output}")
print(f"Total permissions: {len(final_list)}")
