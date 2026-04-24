# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Semantic quality gates to reject corrupt or invalid documents.

# Strings that indicate toxic/error content
TOXIC_STRINGS = [
    "Null pointer exception",
    "NullPointerException",
    "Traceback (most recent call last)",
    "Error:",
    "FATAL:",
    "Segmentation fault",
    "stack overflow",
    "undefined is not a function",
]

MIN_CONTENT_LENGTH = 20


def run_quality_gate(document_dict):
    """
    Returns True if the document passes all quality checks.
    Returns False (and prints a reason) if the document should be rejected.
    """
    content = document_dict.get("content", "")
    doc_id = document_dict.get("document_id", "unknown")

    # Gate 1: Minimum content length
    if len(content.strip()) < MIN_CONTENT_LENGTH:
        print(f"  [QA REJECT] {doc_id}: Content too short ({len(content)} chars).")
        return False

    # Gate 2: Toxic string detection
    for toxic in TOXIC_STRINGS:
        if toxic.lower() in content.lower():
            print(f"  [QA REJECT] {doc_id}: Contains toxic string '{toxic}'.")
            return False

    # Gate 3: Flag discrepancies (warn but don't reject)
    discrepancies = document_dict.get("source_metadata", {}).get("discrepancies", [])
    for d in discrepancies:
        print(f"  [QA WARNING] {doc_id}: {d}")

    return True
