import pandas as pd
import asyncio
import aiohttp
import async_timeout
import sys
import time
from tqdm.asyncio import tqdm_asyncio

OLLAMA_URL = "http://ollamaserver_nuc1:11434/api/generate"
#OLLAMA_URL = "http://localhost:11434/api/generate"
#MODEL_NAME = "codellama:34b"
MODEL_NAME = "codellama:7b"
MAX_CONCURRENT_REQUESTS = 2 #3  # tune for your hardware


# ------------------------------------------------------------
# MODEL HEALTH CHECK
# ------------------------------------------------------------
import requests
import sys

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
            timeout=180  # allow time for model load (34B model)
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
        sys.exit(1)




# ------------------------------------------------------------
# EXTENDED DESCRIPTION GENERATION
# ------------------------------------------------------------
async def get_extended_description(session, priv_type: str, priv_name: str, semaphore):
    """Async version: ask Ollama to return CSV (score, extended_description) and parse it."""

    prompt = f"""
You are an expert in Microsoft Graph API permissions.

For the inputs below, return a CSV with the header exactly:
suggested_privilege_score,extended_description

- suggested_privilege_score: an integer between 1 and 20 (1 = least privilege, 20 = full/admin)
- extended_description: a long, human-readable description of what the privilege allows, security implications, use-cases and guidance.

Return exactly two CSV fields. If the description contains commas or newlines,
enclose it in double-quotes and escape any double-quotes by doubling them.
Do NOT add any other commentary, explanation, or extra rows.

Input:
Privilege Type: {priv_type}
Privilege Name: {priv_name}

Provide the CSV now.
"""

    payload = {"model": MODEL_NAME, "prompt": prompt, "stream": False, "temperature": 0.1}

    async with semaphore:
        for attempt in range(3):
            try:
                async with async_timeout.timeout(300):
                    async with session.post(OLLAMA_URL, json=payload) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            raise RuntimeError(f"Ollama error {resp.status}: {text}")

                        text = await resp.text()

                        # strip optional code fences
                        if text.startswith("```") and text.endswith("```"):
                            text = text.strip("`\n ")

                        # Parse CSV
                        try:
                            reader = __import__("csv").reader(__import__("io").StringIO(text))
                            for row in reader:
                                if not row:
                                    continue
                                if row[0].lower().strip().startswith("suggested"):
                                    continue
                                if len(row) >= 2:
                                    score_raw = row[0].strip()
                                    desc = row[1].strip()
                                    if len(row) > 2:
                                        desc = ",".join([c.strip() for c in row[1:]])
                                    try:
                                        score = int(score_raw)
                                    except Exception:
                                        score = ""
                                    return score, desc
                        except Exception:
                            pass

                        # fallback
                        if "," in text:
                            first, rest = text.split(",", 1)
                            try:
                                score = int(first.strip())
                            except Exception:
                                score = ""
                            return score, rest.strip()

                        return "", text

            except Exception as e:
                if attempt == 2:
                    return ("", f"ERROR after retries: {e}")
                await asyncio.sleep(1 + attempt)


# ------------------------------------------------------------
# MAIN CSV PROCESSING LOGIC
# ------------------------------------------------------------
async def process_csv_async(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)

    required_cols = {
        "privilege_type",
        "privilege_name",
        "privilege_description",
        "privilege_score"
    }

    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"CSV missing required columns: {missing}")

    async with aiohttp.ClientSession() as session:

        # ----- model check BEFORE processing -----
        ok = test_model()
        if not ok:
            print("❌ Exiting because the model is not accessible.")
            sys.exit(1)

        print("🚀 Model OK — beginning CSV processing...\n")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = []

        for _, row in df.iterrows():
            priv_type = row["privilege_type"]
            priv_name = row["privilege_name"]

            tasks.append(get_extended_description(session, priv_type, priv_name, semaphore))

        # Measure elapsed time for the async gather
        start_time = time.perf_counter()
        results = await tqdm_asyncio.gather(*tasks)
        end_time = time.perf_counter()

    # split results into scores and descriptions
    suggested_scores = []
    extended_descriptions = []
    for r in results:
        if isinstance(r, tuple) and len(r) == 2:
            suggested_scores.append(r[0])
            extended_descriptions.append(r[1])
        else:
            suggested_scores.append("")
            extended_descriptions.append(str(r))

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

    elapsed = end_time - start_time
    total = len(tasks)
    avg = elapsed / total if total else 0
    print(f"\n✅ Completed successfully. Output written to: {output_csv}")
    print(f"⏱️ Elapsed time retrieving descriptions: {elapsed:.2f}s — avg {avg:.2f}s per item ({total} items)")


# ------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------
def main():
    asyncio.run(process_csv_async(
        "ms-graph-api_privilege-scraper/graph_permissions_test.csv", 
        "ms-graph-api_privilege-scraper/graph_privileges_with_extended_desc_async-sample.csv"
    ))


if __name__ == "__main__":
    main()
