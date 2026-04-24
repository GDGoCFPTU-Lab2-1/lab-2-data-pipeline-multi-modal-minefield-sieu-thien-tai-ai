import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

# Load .env file from project root (nơi chứa GEMINI_API_KEY)
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")

# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================


def _to_serializable(obj):
    """Recursively convert non-JSON-serializable objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_serializable(i) for i in obj]
    return obj


def _process_and_gate(docs_raw, final_kb, label):
    """
    Accepts either a single dict or a list of dicts.
    Validates via UnifiedDocument, runs quality gate, appends passing docs.
    """
    if docs_raw is None:
        print(f"  [{label}] Skipped (processor returned None).")
        return

    if isinstance(docs_raw, dict):
        docs_raw = [docs_raw]

    accepted = 0
    for raw in docs_raw:
        if not raw:
            continue
        try:
            # Validate against schema (fills in defaults)
            doc = UnifiedDocument(**raw)
            doc_dict = _to_serializable(doc.model_dump())
        except Exception as e:
            print(f"  [{label}] Schema validation failed: {e}")
            continue

        if run_quality_gate(doc_dict):
            final_kb.append(doc_dict)
            accepted += 1

    print(f"  [{label}] {accepted}/{len(docs_raw)} document(s) accepted.")


def main():
    start_time = time.time()
    final_kb = []

    # --- FILE PATH SETUP ---
    pdf_path   = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path  = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path   = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path  = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")

    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")

    # --- PIPELINE DAG ---

    print("[STEP 1] Processing PDF (Gemini API)...")
    pdf_doc = extract_pdf_data(pdf_path)
    _process_and_gate(pdf_doc, final_kb, "PDF")

    print("[STEP 2] Processing Transcript...")
    trans_doc = clean_transcript(trans_path)
    _process_and_gate(trans_doc, final_kb, "Transcript")

    print("[STEP 3] Processing HTML Product Catalog...")
    html_docs = parse_html_catalog(html_path)
    _process_and_gate(html_docs, final_kb, "HTML")

    print("[STEP 4] Processing CSV Sales Records...")
    csv_docs = process_sales_csv(csv_path)
    _process_and_gate(csv_docs, final_kb, "CSV")

    print("[STEP 5] Processing Legacy Code...")
    code_doc = extract_logic_from_code(code_path)
    _process_and_gate(code_doc, final_kb, "LegacyCode")

    # --- SAVE OUTPUT ---
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_kb, f, ensure_ascii=False, indent=2)

    end_time = time.time()
    print(f"\n[DONE] Pipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"[INFO] Total valid documents stored: {len(final_kb)}")
    print(f"[INFO] Output saved to: {output_path}")


if __name__ == "__main__":
    main()
