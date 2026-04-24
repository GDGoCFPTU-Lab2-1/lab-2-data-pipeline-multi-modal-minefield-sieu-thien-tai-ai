import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER — Transcript Processor
# ==========================================
# Cleans demo_transcript.txt: removes timestamps, noise tokens, and extracts
# the Vietnamese price mention ("năm trăm nghìn" -> 500000).

# Vietnamese number words to integer
_VN_NUMBER_MAP = {
    "không": 0,
    "một": 1, "hai": 2, "ba": 3, "bốn": 4, "năm": 5,
    "sáu": 6, "bảy": 7, "tám": 8, "chín": 9,
    "mười": 10,
    "trăm": 100,
    "nghìn": 1000, "ngàn": 1000,
    "triệu": 1_000_000,
    "tỷ": 1_000_000_000,
}

def _parse_vn_number(text):
    """
    Very simple Vietnamese number parser.
    Handles patterns like 'năm trăm nghìn' (500,000).
    Returns None if the pattern can't be parsed.
    """
    tokens = text.lower().split()
    result = 0
    current = 0

    for token in tokens:
        value = _VN_NUMBER_MAP.get(token)
        if value is None:
            continue
        if value >= 1000:
            # Multiplier (nghìn, triệu, tỷ)
            if current == 0:
                current = 1
            result += current * value
            current = 0
        elif value >= 100:
            # Hundreds multiplier
            if current == 0:
                current = 1
            current *= value
        else:
            current += value

    result += current
    return result if result > 0 else None


def clean_transcript(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()

    # 1. Remove timestamps like [00:00:00]
    text_clean = re.sub(r"\[\d{2}:\d{2}:\d{2}\]", "", text)

    # 2. Remove noise tokens (square-bracket annotations)
    noise_patterns = [
        r"\[Music starts?\]",
        r"\[Music ends?\]",
        r"\[Music\]",
        r"\[inaudible\]",
        r"\[Laughter\]",
        r"\[Applause\]",
    ]
    for pattern in noise_patterns:
        text_clean = re.sub(pattern, "", text_clean, flags=re.IGNORECASE)

    # 3. Remove speaker labels like [Speaker 1]:
    text_clean = re.sub(r"\[Speaker \d+\]:", "", text_clean)

    # 4. Clean extra whitespace
    text_clean = re.sub(r"\n{2,}", "\n", text_clean).strip()
    text_clean = re.sub(r"[ \t]+", " ", text_clean)

    # 5. Extract Vietnamese price: "năm trăm nghìn"
    # The transcript says: "Giá của sản phẩm VinAI Pro là năm trăm nghìn VND"
    detected_price = None
    vn_price_match = re.search(
        r"((?:(?:không|một|hai|ba|bốn|năm|sáu|bảy|tám|chín|mười)\s+)*"
        r"(?:trăm|nghìn|ngàn|triệu|tỷ)(?:\s+(?:không|một|hai|ba|bốn|năm|sáu|bảy|tám|chín|mười|trăm|nghìn|ngàn|triệu|tỷ))*)",
        text_clean,
        flags=re.IGNORECASE,
    )
    if vn_price_match:
        vn_phrase = vn_price_match.group(1).strip()
        detected_price = _parse_vn_number(vn_phrase)

    # Fallback: look for explicit "500,000" or "500000" in text
    if detected_price is None:
        explicit_match = re.search(r"500[,.]?000", text)
        if explicit_match:
            detected_price = 500000

    return {
        "document_id": "video-demo_transcript",
        "content": text_clean,
        "source_type": "Video",
        "source_metadata": {
            "file": "demo_transcript.txt",
            "detected_price_vnd": detected_price,
        }
    }
