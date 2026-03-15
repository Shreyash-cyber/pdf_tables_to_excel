"""
Application configuration constants.
"""

import os
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"

# Ensure runtime dirs exist
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Upload limits ─────────────────────────────────────────────────────────
MAX_FILE_SIZE_MB = 200
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
ALLOWED_EXTENSIONS = {".pdf"}

# ══════════════════════════════════════════════════════════════════════════
#  pdfplumber extraction strategies — tried in order, best result wins
# ══════════════════════════════════════════════════════════════════════════

# Strategy 1: Full grid — tables with both horizontal AND vertical lines
STRATEGY_FULL_GRID = {
    "vertical_strategy": "lines_strict",
    "horizontal_strategy": "lines_strict",
    "snap_tolerance": 4,
    "snap_x_tolerance": 4,
    "snap_y_tolerance": 4,
    "join_tolerance": 4,
    "join_x_tolerance": 4,
    "join_y_tolerance": 4,
    "edge_min_length": 3,
    "min_words_vertical": 3,
    "min_words_horizontal": 1,
    "text_tolerance": 3,
    "text_x_tolerance": 3,
    "text_y_tolerance": 3,
}

# Strategy 2: Horizontal lines + text columns (IRDAI-style forms)
# This is the KEY strategy for tables that have horizontal ruling lines
# but NO vertical lines — columns are separated by whitespace.
STRATEGY_HLINES_TEXT = {
    "vertical_strategy": "text",
    "horizontal_strategy": "lines",
    "snap_tolerance": 4,
    "snap_x_tolerance": 4,
    "snap_y_tolerance": 4,
    "join_tolerance": 4,
    "join_x_tolerance": 4,
    "join_y_tolerance": 4,
    "edge_min_length": 10,
    "min_words_vertical": 1,
    "min_words_horizontal": 1,
    "text_tolerance": 3,
    "text_x_tolerance": 3,
    "text_y_tolerance": 3,
    "intersection_tolerance": 15,
}

# Strategy 3: Relaxed lines — some PDFs have thin/partial borders
STRATEGY_LINES_RELAXED = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": 6,
    "snap_x_tolerance": 6,
    "snap_y_tolerance": 6,
    "join_tolerance": 6,
    "join_x_tolerance": 6,
    "join_y_tolerance": 6,
    "edge_min_length": 3,
    "min_words_vertical": 2,
    "min_words_horizontal": 1,
    "text_tolerance": 3,
    "text_x_tolerance": 3,
    "text_y_tolerance": 3,
}

# Strategy 4: Pure text — fully borderless tables
STRATEGY_TEXT_ONLY = {
    "vertical_strategy": "text",
    "horizontal_strategy": "text",
    "snap_tolerance": 5,
    "snap_x_tolerance": 5,
    "snap_y_tolerance": 5,
    "join_tolerance": 5,
    "text_tolerance": 3,
    "text_x_tolerance": 5,
    "text_y_tolerance": 3,
    "min_words_vertical": 2,
    "min_words_horizontal": 1,
}

# Ordered list of strategies to try
PDFPLUMBER_STRATEGIES = [
    ("full_grid", STRATEGY_FULL_GRID),
    ("hlines_text", STRATEGY_HLINES_TEXT),
    ("lines_relaxed", STRATEGY_LINES_RELAXED),
    ("text_only", STRATEGY_TEXT_ONLY),
]

# ── Extraction tuning (Camelot) ───────────────────────────────────────────
CAMELOT_LATTICE_KWARGS = {
    "line_scale": 40,
    "copy_text": ["v"],       # propagate merged-cell text vertically
    "shift_text": [""],
}

CAMELOT_STREAM_KWARGS = {
    "edge_tol": 50,
    "row_tol": 10,
}

# ── Excel styling ─────────────────────────────────────────────────────────
EXCEL_TITLE_FONT_SIZE = 14
EXCEL_HEADER_FONT_SIZE = 11
EXCEL_DATA_FONT_SIZE = 10
EXCEL_DEFAULT_COL_WIDTH = 15

# ── Confidence thresholds ─────────────────────────────────────────────────
# If more than this fraction of cells are empty, consider extraction low-quality
EMPTY_CELL_THRESHOLD = 0.40

# ── Logging ───────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ── Extraction modes ──────────────────────────────────────────────────────
# FAST: pdfplumber only (default, best for typical PDFs)
# ACCURATE: pdfplumber + tabula (good for complex layouts)
# THOROUGH: pdfplumber + tabula + camelot (most comprehensive, slower)
EXTRACTION_MODE = os.getenv("EXTRACTION_MODE", "ACCURATE")

# Enable hybrid extraction for PDFs detected as complex
USE_HYBRID_EXTRACTION = os.getenv("USE_HYBRID_EXTRACTION", "true").lower() in ("true", "1", "yes")

# ── Output format ─────────────────────────────────────────────────────────
# Can be: excel, csv, json, markdown
# Recommended for complex PDFs: json (preserves all structure)
DEFAULT_OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "excel")

# ── Whitespace/Spacing handling ───────────────────────────────────────────
# Preserve newlines in cells (for multi-line values)
PRESERVE_NEWLINES = True

# Normalize multiple spaces to single space
NORMALIZE_SPACES = True

# Minimum word spacing (in pixels) to consider as separate cells
MIN_WORD_SPACING = 5

# Maximum consecutive spaces to preserve (avoids alignment issues)
MAX_CONSECUTIVE_SPACES = 3

