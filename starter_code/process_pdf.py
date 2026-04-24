import os
import time

# ==========================================
# ROLE 2: ETL/ELT BUILDER — PDF Processor
# ==========================================
# Uses Gemini API to extract structured metadata from lecture_notes.pdf.

def extract_pdf_data(file_path):
    # --- FILE CHECK ---
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY environment variable not set.")
        return None

    try:
        from google import genai
        from google.genai import types
    except ImportError:
        print("Error: google-genai package not installed. Run: pip install google-genai")
        return None

    client = genai.Client(api_key=api_key)

    prompt = (
        "You are a document parser. Extract the following from this PDF:\n"
        "1. Title\n"
        "2. Author (or 'Unknown' if not found)\n"
        "3. A concise 3-sentence summary of the main content\n"
        "4. List of main topics covered\n\n"
        "Respond in this exact format:\n"
        "TITLE: <title>\n"
        "AUTHOR: <author>\n"
        "SUMMARY: <summary>\n"
        "TOPICS: <comma-separated list>\n"
    )

    # Exponential backoff for rate limit errors
    max_retries = 5
    delay = 2
    response = None

    print("Uploading PDF to Gemini...")
    with open(file_path, "rb") as f:
        pdf_bytes = f.read()

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-3.1-flash-lite-preview",
                contents=[
                    types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
                    prompt,
                ]
            )
            break
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "quota" in error_str.lower() or "RESOURCE_EXHAUSTED" in error_str:
                print(f"Rate limited. Retrying in {delay}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"Gemini API error: {e}")
                return None

    if response is None:
        print("Failed to get response from Gemini after retries.")
        return None

    raw_text = response.text.strip()

    # Parse structured response
    title = "Unknown Title"
    author = "Unknown"
    summary = raw_text
    topics = []

    for line in raw_text.splitlines():
        if line.startswith("TITLE:"):
            title = line[len("TITLE:"):].strip()
        elif line.startswith("AUTHOR:"):
            author = line[len("AUTHOR:"):].strip()
        elif line.startswith("SUMMARY:"):
            summary = line[len("SUMMARY:"):].strip()
        elif line.startswith("TOPICS:"):
            topics_str = line[len("TOPICS:"):].strip()
            topics = [t.strip() for t in topics_str.split(",") if t.strip()]

    return {
        "document_id": "pdf-lecture_notes",
        "content": summary if summary else raw_text,
        "source_type": "PDF",
        "author": author,
        "source_metadata": {
            "title": title,
            "topics": topics,
            "file": os.path.basename(file_path),
        }
    }
