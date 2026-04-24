import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER — Legacy Code Processor
# ==========================================
# Extracts business rules from docstrings and detects logic discrepancies.

def extract_logic_from_code(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        source_code = f.read()

    # 1. Parse AST to extract function docstrings
    tree = ast.parse(source_code)
    business_rules = []
    discrepancies = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            docstring = ast.get_docstring(node)
            if docstring:
                business_rules.append({
                    "function": node.name,
                    "docstring": docstring.strip()
                })

    # 2. Module-level docstring
    module_docstring = ast.get_docstring(tree) or ""

    # 3. Find "Business Logic Rule" comments via regex
    rule_comments = re.findall(
        r"#\s*(Business Logic Rule\s*\d+[^\\n]*)", source_code
    )

    # 4. Detect tax discrepancy (comment says 8% but code uses 10%)
    # Look for misleading comment pattern
    misleading_pattern = re.search(
        r"(?:calculates|does)\s+(?:VAT|tax)\s+at\s+(\d+)%.*\n.*tax_rate\s*=\s*([0-9.]+)",
        source_code,
        flags=re.IGNORECASE
    )
    if misleading_pattern:
        comment_rate = float(misleading_pattern.group(1))
        code_rate = float(misleading_pattern.group(2)) * 100
        if comment_rate != code_rate:
            discrepancies.append(
                f"Tax discrepancy: comment says {comment_rate:.0f}% but code uses {code_rate:.0f}%"
            )

    # Also check with a simpler regex for the specific pattern in legacy_pipeline.py
    if not discrepancies:
        comment_pct = re.search(r"VAT at (\d+)%", source_code)
        code_pct = re.search(r"tax_rate\s*=\s*([0-9.]+)", source_code)
        if comment_pct and code_pct:
            stated = float(comment_pct.group(1))
            actual = float(code_pct.group(1)) * 100
            if stated != actual:
                discrepancies.append(
                    f"Tax rate discrepancy: comment states {stated:.0f}%, "
                    f"actual code value is {actual:.0f}%"
                )

    # Build content summary
    rules_text = "\n\n".join(
        f"[{r['function']}]: {r['docstring']}" for r in business_rules
    )
    content = f"Module: {module_docstring}\n\nBusiness Rules:\n{rules_text}"
    if discrepancies:
        content += "\n\nDISCREPANCIES DETECTED:\n" + "\n".join(discrepancies)

    return {
        "document_id": "code-legacy_pipeline",
        "content": content,
        "source_type": "Code",
        "author": "Senior Dev (retired)",
        "source_metadata": {
            "file": "legacy_pipeline.py",
            "business_rules": business_rules,
            "rule_comments": rule_comments,
            "discrepancies": discrepancies,
        }
    }
