# 📊 PDF → Excel Converter

A production-grade tool for converting complex financial PDF tables (IRDAI insurance reports) into structured Excel spreadsheets — **with zero data loss**.

## ✨ Features

- **Smart multi-strategy extraction** — pdfplumber with multiple extraction strategies (full grid, text-guided, text-only), automatically picks the best result
- **Advanced table reconstruction** — 12-stage pipeline handles merged columns, multiline cells, continuation rows, garbled headers, corrupted numbers, and displaced data
- **Merged header support** — multi-level headers reconstructed faithfully with Excel merge regions
- **Sub-item aware** — correctly handles section labels with `(a)`, `(b)`, `(i)`, `(ii)` sub-items and their data alignment
- **Exact numeric precision** — financial numbers preserved as-is including parenthesized negatives
- **One table per sheet** — each table gets its own titled worksheet
- **Styled output** — headers, borders, auto-fitted columns
- **Beautiful UI** — minimal white-and-blue Streamlit interface

## 🚀 Quick Start

### Prerequisites

- **Python 3.10+**

### Setup

```bash
# 1. Run setup (creates venv + installs deps)
setup.bat

# 2. Launch the app
run.bat
```

The app opens at **http://localhost:8501**

### Manual Setup

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## 📁 Project Structure

```
├── app.py                  # Streamlit frontend
├── setup.bat               # Windows setup script
├── run.bat                 # Launch script
├── requirements.txt
├── .streamlit/
│   └── config.toml         # White & blue theme
├── backend/
│   ├── __init__.py
│   ├── config.py           # Settings, extraction strategies & tolerances
│   ├── models.py           # TableData, ExtractionResult, MergeRegion
│   └── extractor/
│       ├── __init__.py
│       ├── pdf_engine.py          # Multi-strategy pdfplumber extraction
│       ├── table_reconstructor.py # 12-stage reconstruction pipeline
│       └── excel_writer.py        # Styled openpyxl output
├── uploads/                # Runtime (auto-created)
└── outputs/                # Runtime (auto-created)
```

## 🔧 How It Works

1. **Upload** a financial PDF via the web interface
2. **PDFExtractor** tries multiple pdfplumber strategies per page and picks the best extraction
3. **TableReconstructor** runs a 12-stage pipeline:
   - Fix garbled headers & corrupted numbers
   - Split merged columns & realign displaced data
   - Split numeric data columns & redistribute schedule blobs
   - Expand multiline cells with smart section-header alignment
   - Merge continuation rows (wrapped sub-item labels)
   - Stitch fragmented rows back together
   - Detect multi-level headers & build merge regions
4. **ExcelWriter** produces a styled `.xlsx` with exact structure

## ⚠️ Limitations

- **Scanned PDFs** (image-based) are not supported — text-based PDFs only
- Very unusual table layouts may need extraction tolerance tuning

## 📄 License

MIT
