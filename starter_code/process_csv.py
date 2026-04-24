import pandas as pd
from dateutil import parser as date_parser
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER — CSV Processor
# ==========================================
# Processes sales_records.csv with type traps, duplicates, and mixed date formats.

def _clean_price(value):
    """Convert messy price strings to float. Returns None if unparseable."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    if s in ("N/A", "Liên hệ", "NULL", "", "nan"):
        return None
    # Remove currency symbol and commas
    s = s.replace("$", "").replace(",", "").strip()
    try:
        val = float(s)
        # Reject negative prices (data error)
        if val < 0:
            return None
        return val
    except ValueError:
        return None  # e.g. "five dollars"


def _clean_date(value):
    """Normalize any date string to YYYY-MM-DD. Returns None if unparseable."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        parsed = date_parser.parse(s, dayfirst=False)
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        # Try dayfirst=True as fallback (e.g., "15/01/2026")
        try:
            parsed = date_parser.parse(s, dayfirst=True)
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            return None


def process_sales_csv(file_path):
    df = pd.read_csv(file_path)

    # 1. Remove duplicates by 'id' — keep first occurrence
    df = df.drop_duplicates(subset="id", keep="first")

    # 2. Clean price column
    df["price"] = df["price"].apply(_clean_price)

    # 3. Normalize date column
    df["date_of_sale"] = df["date_of_sale"].apply(_clean_date)

    results = []
    for _, row in df.iterrows():
        doc_id = f"csv-{int(row['id'])}"

        # Build a human-readable content string
        price_str = f"{row['price']:.2f}" if row["price"] is not None else "N/A"
        content = (
            f"Product: {row['product_name']} | Category: {row['category']} | "
            f"Price: {price_str} {row['currency']} | Date: {row['date_of_sale']} | "
            f"Seller: {row['seller_id']}"
        )

        results.append({
            "document_id": doc_id,
            "content": content,
            "source_type": "CSV",
            "author": str(row.get("seller_id", "Unknown")),
            "source_metadata": {
                "product_name": row["product_name"],
                "category": row["category"],
                "price": row["price"],
                "currency": row["currency"],
                "date_of_sale": row["date_of_sale"],
                "seller_id": row.get("seller_id"),
                "stock_quantity": None if pd.isna(row.get("stock_quantity", float("nan"))) else int(row["stock_quantity"]),
            }
        })

    return results
