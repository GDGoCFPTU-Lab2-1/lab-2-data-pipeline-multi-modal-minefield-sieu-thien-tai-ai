from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# v1 Schema — Unified contract for all data sources.

class UnifiedDocument(BaseModel):
    document_id: str                          # Unique ID, e.g. "csv-1", "html-SP001"
    content: str                              # Main text content
    source_type: str                          # 'PDF', 'Video', 'HTML', 'CSV', 'Code'
    author: Optional[str] = "Unknown"
    timestamp: Optional[datetime] = None

    # Flexible bag for source-specific extras
    source_metadata: dict = Field(default_factory=dict)
