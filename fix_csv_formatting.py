import pandas as pd
import re

# -----------------------------
# Configuration
# -----------------------------
INPUT_CSV = "ms-graph-api_privilege-scraper/graph-privileges_extended-desc_two-pass_34b-2.csv"
OUTPUT_CSV = "ms-graph-api_privilege-scraper/graph-privileges_extended-desc_score_final.csv"

TEXT_COLUMN = "extended_description"

# -----------------------------
# Load CSV
# -----------------------------
df = pd.read_csv(
    INPUT_CSV,
    dtype=str,          # preserve text exactly
    keep_default_na=False
)

if TEXT_COLUMN not in df.columns:
    raise ValueError(f"Column '{TEXT_COLUMN}' not found in CSV")

# -----------------------------
# Clean newline / carriage returns
# -----------------------------
def clean_text(value):
    if not isinstance(value, str):
        return value

    # Replace CR, LF, CRLF with a single space
    value = re.sub(r"[\r\n]+", " ", value)

    # Collapse multiple spaces
    value = re.sub(r"\s{2,}", " ", value)

    return value.strip()

df[TEXT_COLUMN] = df[TEXT_COLUMN].apply(clean_text)

# -----------------------------
# Write strict CSV
# -----------------------------
df.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding="utf-8",
    quoting=1,           # csv.QUOTE_ALL
    lineterminator="\n"
)

print(f"✅ Clean CSV written to: {OUTPUT_CSV}")
