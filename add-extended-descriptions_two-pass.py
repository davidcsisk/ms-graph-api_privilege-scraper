# Add extended descriptions to these Graph API privileges using Ollama's Code Llama model.
# Requires Ollama running locally with the Code Llama model downloaded. 
# Model: ollama run codellama:34b 
# Note: The above model requires 17.6Gb of free memory to be able to run.
# I'm running Ollama on a local linux NUC server with 64Gb RAM.

import pandas as pd
import requests
import sys
import time
import csv
import io
from tqdm import tqdm   # optional but nice progress bar

#OLLAMA_URL = "http://ollamaserver_nuc1:11434/api/generate"
OLLAMA_URL = "http://localhost:11434/api/generate"
#MODEL_NAME = "codellama:34b"
MODEL_NAME = "codellama:7b"


def test_model():
    """
    Synchronously test whether the Ollama model is accessible and responding.
    Exits the program if the test fails.
    """

    print("🔍 Testing model availability...")

    test_prompt = (
        "Can you tell me your model name, what you specialize in, "
        "if you have been fine-tuned for any specific purpose, "
        "and the date of your most recent training data?"
    )

    payload = {
        "model": MODEL_NAME,
        "prompt": test_prompt,
        "stream": False,
        "temperature": 0.1
    }

    try:
        response = requests.post(
            OLLAMA_URL,
            json=payload,
            timeout=360  # allow time for model load (34B model)
        )

        response.raise_for_status()
        data = response.json()

        response_text = data.get("response", "").strip()
        if not response_text:
            raise RuntimeError("Model returned an empty response.")

        print("\n✅ MODEL RESPONSE:")
        print("--------------------------------------------")
        print(response_text)
        print("--------------------------------------------\n")

        return True

    except Exception as e:
        print("\n❌ ERROR: Model test failed")
        print(f"Exception type: {type(e)}")
        print(f"Exception message: {e}")
        return False


def get_extended_description(priv_type: str, priv_name: str):
    """Ask Ollama for an extended description only.

    The model is instructed to return a CSV with the header exactly:
    extended_description

    Returns the description string (or the raw text fallback).
    """

    prompt = f"""
You are an expert in Microsoft Graph API permissions.

For the inputs below, return a CSV with the header exactly:
extended_description

- extended_description: a long, human-readable description of what the privilege allows, security implications, use-cases and guidance.

Return exactly one CSV field (the description). If the description contains commas or newlines,
enclose it in double-quotes and escape any double-quotes by doubling them.
Do NOT add any other commentary, explanation, or extra rows.

Input:
Privilege Type: {priv_type}
Privilege Name: {priv_name}

Provide the CSV now.
"""

    payload = {"model": MODEL_NAME, "prompt": prompt, "stream": False, "temperature": 0.1}
    r = requests.post(OLLAMA_URL, json=payload, timeout=300)
    r.raise_for_status()
    text = r.json().get("response", "").strip()

    # strip optional code fences
    if text.startswith("```") and text.endswith("```"):
        text = text.strip("`\n ")

    # Parse CSV output robustly (expecting a single field)
    try:
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if not row:
                continue
            # If header present, skip it
            if row[0].lower().strip().startswith("extended"):
                continue
            # join all columns into description (handles accidental extra commas)
            desc = ",".join([c.strip() for c in row])
            return desc
    except Exception:
        pass

    # Final fallback: return whole text as description
    return text



def get_suggested_score(priv_type: str, priv_name: str):
    """Ask Ollama for a single integer suggested_privilege_score (1-20).

    The model is instructed to return a CSV with the header exactly:
    suggested_privilege_score

    Returns an integer score or an empty string on failure/invalid value.
    """

    prompt = f"""
You are an expert in Microsoft Graph API permissions.

For the inputs below, return a CSV with the header exactly:
suggested_privilege_score

- suggested_privilege_score: an integer between 1 and 20 (1 = least privilege, 20 = full/admin)

Return exactly one CSV field that is the integer score. Do NOT add any commentary, explanation, or extra rows.

Input:
Privilege Type: {priv_type}
Privilege Name: {priv_name}

Provide the CSV now.
"""

    payload = {"model": MODEL_NAME, "prompt": prompt, "stream": False, "temperature": 0.1}
    r = requests.post(OLLAMA_URL, json=payload, timeout=120)
    r.raise_for_status()
    text = r.json().get("response", "").strip()

    # strip optional code fences
    if text.startswith("```") and text.endswith("```"):
        text = text.strip("`\n ")

    # Parse CSV output robustly
    try:
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if not row:
                continue
            if row[0].lower().strip().startswith("suggested"):
                continue
            first = row[0].strip()
            try:
                score = int(first)
                if 1 <= score <= 20:
                    return score
                else:
                    return ""
            except Exception:
                continue
    except Exception:
        pass

    # Fallback: search for an integer in the text
    import re
    m = re.search(r"\\b(\\d{1,2})\\b", text)
    if m:
        try:
            score = int(m.group(1))
            if 1 <= score <= 20:
                return score
        except Exception:
            pass

    return ""





def process_csv(input_csv: str, output_csv: str):
    """
    Read the input CSV, generate extended descriptions,
    and write the augmented CSV to disk.
    """

    # ----- model check BEFORE processing -----
    if not test_model():
        print("❌ Exiting because the model is not accessible.")
        sys.exit(1)

    df = pd.read_csv(input_csv)

    # Validate columns
    required_cols = {
        "privilege_type",
        "privilege_name",
        "privilege_description",
        "privilege_score"
    }

    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"CSV is missing required columns: {missing}")

    extended_descriptions = []
    suggested_scores = []

    # Measure elapsed time for retrieving extended descriptions
    start_time = time.perf_counter()
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing privileges"):
        priv_type = row["privilege_type"]
        priv_name = row["privilege_name"]

        try:
            ext_desc = get_extended_description(priv_type, priv_name)
        except Exception as e:
            ext_desc = f"ERROR: {e}"

        try:
            score = get_suggested_score(priv_type, priv_name)
        except Exception as e:
            score = ""

        suggested_scores.append(score)
        extended_descriptions.append(ext_desc)
    end_time = time.perf_counter()

    elapsed = end_time - start_time
    total = len(df)
    avg = elapsed / total if total else 0

    # Insert suggested score column and extended description
    df["suggested_privilege_score"] = suggested_scores
    df["extended_description"] = extended_descriptions

    # Enforce exact column order
    desired_cols = [
        "privilege_type",
        "privilege_name",
        "privilege_description",
        "privilege_score",
        "suggested_privilege_score",
        "extended_description",
    ]
    missing = [c for c in desired_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns before write: {missing}")
    df = df[desired_cols]

    df.to_csv(output_csv, index=False)

    print(f"\n✅ Completed. Output written to: {output_csv}")
    print(f"⏱️ Elapsed time retrieving descriptions: {elapsed:.2f}s — avg {avg:.2f}s per item ({total} items)")


if __name__ == "__main__":
    process_csv("ms-graph-api_privilege-scraper/graph_permissions_test.csv", 
                "ms-graph-api_privilege-scraper/graph-privileges_extended-desc_two-pass-sample.csv")
