"""
Multi-format output handler for extracted tables.

Supports: Excel (.xlsx), CSV, JSON, and Markdown.
Useful for complex PDFs where different formats may preserve structure better.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Literal

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

from backend.models import ExtractionResult, TableData

logger = logging.getLogger(__name__)

OutputFormat = Literal["excel", "csv", "json", "markdown"]


class OutputFormatter:
    """Convert extracted tables to various formats."""

    @staticmethod
    def write_format(
        result: ExtractionResult,
        output_path: str | Path,
        format: OutputFormat = "excel"
    ) -> Path:
        """
        Write extraction results in specified format.
        
        Args:
            result: ExtractionResult containing tables
            output_path: Output file path (will be adjusted for format if needed)
            format: 'excel', 'csv', 'json', or 'markdown'
        
        Returns:
            Path to created file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "excel":
            from backend.extractor.excel_writer import ExcelWriter
            return ExcelWriter().write(result, output_path)
        
        elif format == "csv":
            return OutputFormatter._write_csv(result, output_path)
        
        elif format == "json":
            return OutputFormatter._write_json(result, output_path)
        
        elif format == "markdown":
            return OutputFormatter._write_markdown(result, output_path)
        
        else:
            raise ValueError(f"Unsupported format: {format}")

    @staticmethod
    def _write_csv(result: ExtractionResult, output_path: Path) -> Path:
        """Write tables as separate CSV files (one per table)."""
        if not HAS_PANDAS:
            raise ImportError("pandas required for CSV output")

        output_path = output_path.with_suffix(".zip")
        output_dir = output_path.parent / output_path.stem
        output_dir.mkdir(parents=True, exist_ok=True)

        csv_files = []
        for idx, table in enumerate(result.tables, 1):
            if table.is_empty:
                continue

            # Create DataFrame from table
            all_rows = table.headers + table.rows
            df = pd.DataFrame(all_rows[1:], columns=all_rows[0] if all_rows else None)

            # Safe filename
            safe_title = "".join(c if c.isalnum() or c in "_-" else "_" for c in table.title)
            safe_title = safe_title[:50] or f"table_{idx}"
            csv_file = output_dir / f"{idx:03d}_{safe_title}.csv"

            df.to_csv(csv_file, index=False)
            csv_files.append(csv_file)
            logger.info("CSV written: %s", csv_file)

        # Create zip with all CSVs
        import shutil
        if csv_files:
            shutil.make_archive(str(output_path.with_suffix("").with_name(output_path.stem)), 
                               "zip", output_dir)
            logger.info("CSV zip created: %s", output_path)
            return output_path

        return output_path

    @staticmethod
    def _write_json(result: ExtractionResult, output_path: Path) -> Path:
        """Write tables as JSON (one object per table)."""
        output_path = output_path.with_suffix(".json")

        data = {
            "filename": result.filename,
            "page_count": result.page_count,
            "table_count": len([t for t in result.tables if not t.is_empty]),
            "errors": result.errors,
            "tables": []
        }

        for table in result.tables:
            if table.is_empty:
                continue

            table_data = {
                "title": table.title,
                "page": table.page_number,
                "confidence": table.confidence,
                "headers": table.headers,
                "rows": table.rows,
                "row_count": len(table.rows),
                "column_count": len(table.headers[0]) if table.headers else 0,
            }
            data["tables"].append(table_data)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info("JSON written: %s", output_path)
        return output_path

    @staticmethod
    def _write_markdown(result: ExtractionResult, output_path: Path) -> Path:
        """Write tables as Markdown (GitHub-flavored markdown tables)."""
        output_path = output_path.with_suffix(".md")

        lines = [
            f"# PDF Table Extraction Report\n",
            f"**File:** {result.filename}\n",
            f"**Pages:** {result.page_count}\n",
            f"**Tables Found:** {len([t for t in result.tables if not t.is_empty])}\n",
        ]

        if result.errors:
            lines.append(f"\n## Errors\n")
            for error in result.errors:
                lines.append(f"- {error}\n")

        for idx, table in enumerate(result.tables, 1):
            if table.is_empty:
                continue

            lines.append(f"\n## Table {idx}: {table.title}\n")
            lines.append(f"*Page {table.page_number} | Confidence: {table.confidence:.0%}*\n\n")

            all_rows = table.headers + table.rows
            if not all_rows:
                continue

            # Header
            header = all_rows[0]
            lines.append("| " + " | ".join(str(c)[:30] for c in header) + " |\n")
            lines.append("|" + "|".join(["---"] * len(header)) + "|\n")

            # Data rows
            for row in all_rows[1:]:
                lines.append("| " + " | ".join(str(c)[:30] for c in row) + " |\n")

        with open(output_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        logger.info("Markdown written: %s", output_path)
        return output_path


def detect_best_format(pdf_complexity: float) -> OutputFormat:
    """
    Recommend best format based on PDF complexity.
    
    Simple PDFs: Excel (structured)
    Complex PDFs: JSON (preserves all structure) or CSV (simplest)
    """
    if pdf_complexity < 0.3:
        return "excel"  # Simple, well-structured
    elif pdf_complexity < 0.7:
        return "excel"  # Still good for Excel
    else:
        return "json"  # Complex layout - JSON preserves structure better
