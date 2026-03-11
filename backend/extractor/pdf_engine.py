"""
Clean PDF table extraction engine.

Strategy cascade:
  1. Default pdfplumber settings (works for most PDFs with rects/lines)
  2. 'lines' strategy (stricter line detection)
  3. Explicit rect-edge strategy (uses rect edges as table boundaries)
  4. NO text strategy (avoids word shattering)
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pdfplumber

from backend.models import ExtractionResult, TableData

logger = logging.getLogger(__name__)


class PDFExtractor:
    """Extracts tables from machine-generated PDFs using pdfplumber."""

    def __init__(self, pdf_path: str | Path) -> None:
        self.pdf_path = Path(pdf_path)
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

    def extract(self, progress_callback: callable[[int, int], None] | None = None) -> ExtractionResult:
        """
        Extract all tables from all pages.
        
        Args:
            progress_callback: Function taking (current_page, total_pages)
        """
        result = ExtractionResult(filename=self.pdf_path.name)

        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                result.page_count = len(pdf.pages)
                logger.info("Processing %s (%d pages)", self.pdf_path.name, result.page_count)

                for page_idx, page in enumerate(pdf.pages, start=1):
                    if progress_callback:
                        progress_callback(page_idx, result.page_count)
                    elif page_idx % 25 == 0 or page_idx == 1:
                        logger.info("Page %d / %d ...", page_idx, result.page_count)

                    page_tables = self._extract_page(page, page_idx)
                    result.tables.extend(page_tables)

        except Exception as exc:
            logger.exception("Extraction failed: %s", exc)
            result.errors.append(str(exc))

        logger.info("Done: %d tables from %d pages", len(result.tables), result.page_count)
        return result

    def _extract_page(self, page: Any, page_number: int) -> list[TableData]:
        """
        Extract tables from a single page.
        Try default → lines → word-grid fallback.
        Falls back to word-grid when pdfplumber creates mega-cells
        (a single cell containing many newlines, indicating broken extraction).
        """
        # Strategy 1: pdfplumber defaults (rects + lines)
        tables = self._try_extract(page, page_number, settings=None, label="default")
        if tables and not _has_mega_cells(tables):
            return tables

        # Strategy 2: explicit 'lines' with relaxed tolerance
        tables = self._try_extract(page, page_number, settings={
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "snap_tolerance": 5,
            "join_tolerance": 5,
        }, label="lines")
        if tables and not _has_mega_cells(tables):
            return tables

        # Strategy 3: Word-position grid — extract words and place them
        # into a grid based on (x, y) coordinates. Uses vertical edges
        # from the PDF as column boundary hints. This captures ALL content
        # including label columns that lack line boundaries.
        tables = self._try_word_grid(page, page_number)
        if tables:
            return tables

        # No tables found at all — skip this page
        return []

    def _try_word_grid(self, page: Any, page_number: int) -> list[TableData] | None:
        """
        Build a table from word positions when line-based extraction fails.

        Steps:
          1. Extract all words with bounding boxes
          2. Determine column boundaries from vertical edges (+ text margins)
          3. Cluster words into rows by y-position
          4. Place each word into the correct (row, col) cell
        """
        from collections import defaultdict

        try:
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            if not words or len(words) < 5:
                return None

            # --- Column boundaries ---
            col_bounds = self._detect_column_boundaries(page, words)
            if not col_bounds or len(col_bounds) < 3:
                return None  # Need at least 2 columns

            ncols = len(col_bounds) - 1

            # --- Row clustering (group words within 5px vertically) ---
            words_sorted = sorted(words, key=lambda w: w['top'])
            row_groups: list[list[dict]] = []
            current = [words_sorted[0]]
            for w in words_sorted[1:]:
                if abs(w['top'] - current[0]['top']) <= 5:
                    current.append(w)
                else:
                    row_groups.append(sorted(current, key=lambda ww: ww['x0']))
                    current = [w]
            row_groups.append(sorted(current, key=lambda ww: ww['x0']))

            if len(row_groups) < 2:
                return None

            # --- Build grid ---
            def _get_col(x0: float) -> int:
                for ci in range(ncols):
                    if x0 < col_bounds[ci + 1]:
                        return ci
                return ncols - 1

            grid: list[list[str]] = []
            for row_words in row_groups:
                cells: dict[int, list[str]] = defaultdict(list)
                for w in row_words:
                    ci = _get_col(w['x0'])
                    cells[ci].append(w['text'])
                row = [' '.join(cells.get(ci, [])) for ci in range(ncols)]
                if any(c.strip() for c in row):
                    grid.append(row)

            if not grid or len(grid) < 2:
                return None

            # Pad rows to same width
            maxcols = max(len(r) for r in grid)
            for r in grid:
                while len(r) < maxcols:
                    r.append("")

            title = f"Table 1 (Page {page_number})"
            table = TableData(
                title=title,
                headers=[grid[0]],
                rows=grid[1:],
                page_number=page_number,
                confidence=0.80,
            )

            logger.info("Word-grid P%d: %d rows × %d cols", page_number,
                         len(grid), ncols)
            return [table]

        except Exception as exc:
            logger.debug("Word-grid failed on P%d: %s", page_number, exc)
            return None

    @staticmethod
    def _detect_column_boundaries(page: Any, words: list[dict]) -> list[float]:
        """
        Detect column boundaries using vertical edges from the PDF plus
        text extent margins.

        Returns a sorted list of x-positions defining column boundaries
        (n boundaries → n-1 columns).
        """
        min_x = min(w['x0'] for w in words) - 1
        max_x = max(w['x1'] for w in words) + 1

        # Use vertical edges as column boundary hints
        v_positions = sorted(set(round(e['x0']) for e in page.edges
                                 if e.get('orientation') == 'v'))
        if v_positions:
            merged = [v_positions[0]]
            for v in v_positions[1:]:
                if v - merged[-1] < 20:
                    merged[-1] = (merged[-1] + v) // 2
                else:
                    merged.append(v)
        else:
            merged = []

        # Build boundaries: left margin + edges + right margin
        bounds = [min_x] + merged + [max_x]

        # Remove boundaries too close together
        clean: list[float] = [bounds[0]]
        for b in bounds[1:]:
            if b - clean[-1] >= 15:
                clean.append(b)
            else:
                clean[-1] = max(clean[-1], b)

        # If no edges found, try to detect columns from text gaps
        if not merged:
            clean = _detect_cols_from_text_gaps(words, min_x, max_x)

        return clean if len(clean) >= 3 else []

    @staticmethod
    def _quality_ok(tables: list[TableData]) -> bool:
        """Check if extraction results are reasonable (not shattered text)."""
        if not tables:
            return False
        for t in tables:
            all_rows = t.headers + t.rows
            if not all_rows:
                continue
            ncols = len(all_rows[0])
            # Shattered text produces many columns (>30) — reject
            if ncols > 30:
                return False
            # Check first non-empty data row for shattered cells
            for row in all_rows[:5]:
                non_empty = [c for c in row if c.strip()]
                if non_empty:
                    # If average cell length < 3 chars, it's shattered
                    avg_len = sum(len(c) for c in non_empty) / len(non_empty)
                    if avg_len < 3 and ncols > 10:
                        return False
                    break
        return True

    def _try_extract(
        self,
        page: Any,
        page_number: int,
        settings: dict | None,
        label: str,
    ) -> list[TableData] | None:
        """Try a single extraction strategy. Returns list of tables or None."""
        try:
            if settings:
                found = page.find_tables(table_settings=settings)
            else:
                found = page.find_tables()

            if not found:
                return None

            tables: list[TableData] = []

            for tidx, tobj in enumerate(found):
                raw = tobj.extract()
                if not raw:
                    continue

                # Fix sparse reference columns (e.g. Schedule L-4/L-5/L-6/L-7)
                # in mega-rows where pdfplumber merges all data into one row.
                _fix_sparse_cell_alignment(page, tobj, raw)

                cleaned = _clean_raw(raw)
                if not cleaned or len(cleaned) < 2:
                    continue

                ncols = len(cleaned[0])
                if ncols < 2:
                    continue

                confidence = _confidence(cleaned)
                title = self._get_title(page, tobj, page_number, tidx)

                tables.append(TableData(
                    title=title,
                    headers=[cleaned[0]],
                    rows=cleaned[1:],
                    page_number=page_number,
                    confidence=confidence,
                ))

            return tables if tables else None

        except Exception as exc:
            logger.debug("Strategy '%s' failed on P%d: %s", label, page_number, exc)
            return None

    def _get_title(
        self, page: Any, table_obj: Any, page_number: int, table_idx: int
    ) -> str:
        """Get title text above the table."""
        default = f"Table {table_idx + 1} (Page {page_number})"
        try:
            bbox = table_obj.bbox
            if not bbox:
                return default
            x0, top, x1, _ = bbox
            search_top = max(0, top - 50)
            cropped = page.within_bbox((x0, search_top, x1, top), relative=False)
            chars = cropped.chars if cropped else []
            if not chars:
                return default

            from collections import defaultdict
            lines: dict[int, list] = defaultdict(list)
            for c in chars:
                lines[int(round(c["top"]))].append(c)

            for ykey in sorted(lines.keys(), reverse=True):
                text = "".join(
                    c["text"] for c in sorted(lines[ykey], key=lambda c: c["x0"])
                ).strip()
                if text and len(text) > 3:
                    return text

            return default
        except Exception:
            return default


# ── Utility functions ─────────────────────────────────────────────────────

def _clean_raw(raw: list[list]) -> list[list[str]]:
    """
    Clean raw table: None→'', preserve newlines, pad cols.
    Preserves internal blank rows (visual separators in the PDF grid)
    while trimming leading/trailing blank rows.
    """
    if not raw:
        return []

    cleaned = []
    for row in raw:
        r = []
        for c in (row or []):
            if c is None:
                r.append("")
            elif isinstance(c, str):
                r.append(c.strip())
            else:
                r.append(str(c).strip())
        cleaned.append(r)

    # Trim leading blank rows
    while cleaned and not any(c for c in cleaned[0]):
        cleaned.pop(0)
    # Trim trailing blank rows
    while cleaned and not any(c for c in cleaned[-1]):
        cleaned.pop()

    if not cleaned:
        return []

    maxcols = max(len(r) for r in cleaned)
    for r in cleaned:
        while len(r) < maxcols:
            r.append("")

    return cleaned


def _confidence(table: list[list[str]]) -> float:
    """
    Confidence score reflecting structural integrity rather than raw cell density.
    Sparse financial tables are normal.
    """
    total = sum(len(r) for r in table)
    if total == 0:
        return 0.0
        
    empty = sum(1 for r in table for c in r if not c.replace("\n", "").strip())
    density = 1 - (empty / total)
    
    # Base confidence is high if we successfully extracted a table without shattering
    confidence = 0.85
    
    # Reward for good structural density (not too sparse)
    if density > 0.3:
        confidence += 0.10
        
    # Reward for having a good mix of numbers (actual data extracted)
    numeric_cells = sum(1 for r in table for c in r if any(char.isdigit() for char in c))
    if numeric_cells > 0:
        confidence += 0.04
        
    # Apply a small penalty for extremely empty tables (density < 10%)
    if density < 0.1:
        confidence -= 0.20
        
    # Cap between 0 and 0.99
    return max(0.0, min(0.99, confidence))


def _has_mega_cells(tables: list[TableData]) -> bool:
    """
    Detect if pdfplumber produced a 'mega-cell' — a single cell containing
    far more newlines than the table has rows, meaning the extraction crammed
    an entire column into one cell. This is a sign that the table boundaries
    were wrong and word-grid extraction will produce better results.
    """
    for t in tables:
        all_rows = t.headers + t.rows
        nrows = len(all_rows)
        if nrows < 3:
            continue
        for row in all_rows:
            for cell in row:
                if not cell:
                    continue

                line_count = cell.count('\n') + 1
                text_len = len(cell)

                # Primary trigger: one cell contains roughly most of the table's rows.
                strong_threshold = max(15, int(nrows * 0.70))
                if line_count >= strong_threshold:
                    logger.info(
                        "Mega-cell detected on P%d (%d lines in cell vs %d rows) — falling back to word-grid",
                        t.page_number,
                        line_count,
                        nrows,
                    )
                    return True

                # Secondary trigger: very long multiline blobs are almost always
                # extraction failures even if line_count is just below row count.
                if line_count >= 18 and text_len >= 450:
                    logger.info(
                        "Mega-cell detected on P%d (long multiline blob: %d lines, %d chars) — falling back to word-grid",
                        t.page_number,
                        line_count,
                        text_len,
                    )
                    return True
    return False


def _fix_sparse_cell_alignment(page: Any, tobj: Any, raw: list[list]) -> None:
    """
    Fix alignment of sparse reference columns in mega-rows.

    When pdfplumber merges all table rows into one (a "mega-row"),
    a column like Schedule ends up as "L-4\\nL-5\\nL-6\\nL-7" (4 lines)
    paired with a label column of 56 lines.  Simple line-by-line expansion
    would place L-5 on "(a) Premium" instead of "Commission".

    This function uses word Y-positions from the PDF to insert the correct
    number of empty lines so that each value aligns with its proper label row.
    """
    from collections import defaultdict

    for ri, row in enumerate(raw):
        if not row:
            continue

        # Find the mega-column (column with the most text lines)
        mega_ci = -1
        mega_n = 0
        for ci, cell in enumerate(row):
            if cell and isinstance(cell, str):
                n = cell.count('\n') + 1
                if n > mega_n:
                    mega_n = n
                    mega_ci = ci

        if mega_n < 10 or mega_ci < 0:
            continue

        # Look for sparse reference columns in the same row
        for ci, cell in enumerate(row):
            if ci == mega_ci or not cell or not isinstance(cell, str):
                continue

            vals = [ln.strip() for ln in cell.split('\n') if ln.strip()]
            if len(vals) < 2 or len(vals) > mega_n // 3:
                continue

            # All non-empty lines must look like reference codes (L-4, S-5, …)
            if not all(re.match(r'^[A-Z]+-?\d', v) for v in vals):
                continue

            try:
                row_obj = tobj.rows[ri]
                sparse_bbox = row_obj.cells[ci]
                mega_bbox = row_obj.cells[mega_ci]
                if not sparse_bbox or not mega_bbox:
                    continue

                # --- Word Y-positions for the sparse column ---
                sp_words = page.within_bbox(sparse_bbox).extract_words()
                if not sp_words:
                    continue
                sp_items = sorted(
                    ((w['top'], w['text']) for w in sp_words),
                    key=lambda x: x[0],
                )
                # Merge words on the same visual line (within 5 px)
                merged_sparse: list[tuple[float, str]] = [sp_items[0]]
                for y, t in sp_items[1:]:
                    if y - merged_sparse[-1][0] < 5:
                        # Use separator: space for same-line chars, newline
                        # for reference codes that should stay separate
                        prev = merged_sparse[-1][1]
                        sep = ' ' if not re.match(r'^[A-Z]+-?\d', t) else '\n'
                        merged_sparse[-1] = (merged_sparse[-1][0],
                                             prev + sep + t)
                    else:
                        merged_sparse.append((y, t))

                # --- Word Y-positions for the mega-column (label lines) ---
                mega_words = page.within_bbox(mega_bbox).extract_words()
                if not mega_words:
                    continue
                mega_ys_raw = sorted(set(round(w['top']) for w in mega_words))
                # Merge nearby Y groups (within 8 px → less than half a line)
                merged_mega: list[int] = [mega_ys_raw[0]]
                for y in mega_ys_raw[1:]:
                    if y - merged_mega[-1] >= 8:
                        merged_mega.append(y)

                n_text_lines = mega_n  # from original text split

                # --- Map each sparse value to the correct text-line index ---
                aligned = [''] * n_text_lines
                for si, (sy, stext) in enumerate(merged_sparse):
                    # Y-range: first value starts from the first mega line
                    y_start = (merged_mega[0]
                               if si == 0
                               else sy)
                    y_end = (merged_sparse[si + 1][0]
                             if si < len(merged_sparse) - 1
                             else float('inf'))

                    # First mega-line Y in [y_start - tol, y_end)
                    # Small tolerance (3px) handles sub-pixel positioning
                    target_mega_idx = None
                    for mi, my in enumerate(merged_mega):
                        if my >= y_start - 3 and my < y_end:
                            target_mega_idx = mi
                            break

                    if target_mega_idx is None:
                        target_mega_idx = si  # fallback

                    # Scale to text-line index when counts differ
                    if (len(merged_mega) != n_text_lines
                            and len(merged_mega) > 1):
                        target = round(
                            target_mega_idx
                            / (len(merged_mega) - 1)
                            * (n_text_lines - 1)
                        )
                    else:
                        target = min(target_mega_idx, n_text_lines - 1)

                    if 0 <= target < n_text_lines:
                        aligned[target] = stext

                # Trim trailing empties (they're naturally padded later)
                while aligned and not aligned[-1]:
                    aligned.pop()

                raw[ri][ci] = '\n'.join(aligned)

            except Exception:
                continue  # leave the cell as-is on any failure


def _detect_cols_from_text_gaps(words: list[dict], min_x: float, max_x: float) -> list[float]:
    """
    Detect column boundaries from gaps in x-positions of words.
    Used when no vertical edges are available in the PDF.
    """
    if not words:
        return []

    # Collect all x0 positions
    all_x0 = sorted(set(round(w['x0']) for w in words))
    if len(all_x0) < 5:
        return []

    # Find significant gaps (> 40px suggests a column boundary)
    gaps: list[tuple[float, float, float]] = []
    for i in range(1, len(all_x0)):
        gap_size = all_x0[i] - all_x0[i - 1]
        if gap_size >= 40:
            midpoint = (all_x0[i - 1] + all_x0[i]) / 2
            gaps.append((midpoint, gap_size, all_x0[i]))

    if not gaps:
        return []

    # Use the largest gaps as column boundaries
    gaps.sort(key=lambda g: g[1], reverse=True)
    # Take up to 10 column boundaries
    boundaries = sorted(g[0] for g in gaps[:10])

    return [min_x] + boundaries + [max_x]
