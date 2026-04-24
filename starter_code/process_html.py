from bs4 import BeautifulSoup
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER — HTML Processor
# ==========================================
# Extracts product rows from the main-catalog table; ignores nav/footer/ads.

def _clean_html_price(value):
    """Convert HTML price strings to float. Returns None if not a number."""
    s = str(value).strip()
    if s in ("N/A", "Liên hệ", "", "nan"):
        return None
    # Remove currency label and formatting: "28,500,000 VND" -> 28500000.0
    s = re.sub(r"[^\d.]", "", s.replace(",", ""))
    try:
        return float(s)
    except ValueError:
        return None


def parse_html_catalog(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    table = soup.find("table", {"id": "main-catalog"})
    if not table:
        print("Warning: #main-catalog table not found in HTML.")
        return []

    results = []
    rows = table.find("tbody").find_all("tr")

    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 6:
            continue

        product_id, name, category, price_raw, stock_raw, rating_raw = cols[:6]

        price = _clean_html_price(price_raw)

        # Stock quantity — handle negative (data error)
        try:
            stock = int(stock_raw)
            if stock < 0:
                stock = None
        except (ValueError, TypeError):
            stock = None

        content = (
            f"Product: {name} | Category: {category} | "
            f"Price: {price_raw} | Stock: {stock_raw} | Rating: {rating_raw}"
        )

        results.append({
            "document_id": f"html-{product_id}",
            "content": content,
            "source_type": "HTML",
            "source_metadata": {
                "product_id": product_id,
                "name": name,
                "category": category,
                "price": price,
                "price_raw": price_raw,
                "stock": stock,
                "rating": rating_raw,
            }
        })

    return results
