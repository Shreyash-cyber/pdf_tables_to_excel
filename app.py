"""
PDF → Excel Converter — Enterprise-Grade Streamlit Application

Clean, single-page design for converting PDF tables to multiple formats.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import streamlit as st
import base64

# ── Ensure project root is on sys.path ────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import OUTPUT_DIR, UPLOAD_DIR
from backend.extractor.pdf_engine import PDFExtractor
from backend.extractor.table_reconstructor import TableReconstructor
from backend.extractor.output_formatter import OutputFormatter

# ── Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)s │ %(levelname)s │ %(message)s",
)
logger = logging.getLogger(__name__)


def get_base64_image(image_path):
    """Convert image to base64 for embedding."""
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()


# ══════════════════════════════════════════════════════════════════════════
#  Page Configuration
# ══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Bajaj Life | PDF to Excel Converter",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Initialize Session State
if 'is_processed' not in st.session_state:
    st.session_state.is_processed = False
if 'extraction_result' not in st.session_state:
    st.session_state.extraction_result = None
if 'output_path' not in st.session_state:
    st.session_state.output_path = None
if 'elapsed_time' not in st.session_state:
    st.session_state.elapsed_time = 0
if 'selected_format' not in st.session_state:
    st.session_state.selected_format = "excel"


# ══════════════════════════════════════════════════════════════════════════
#  CSS Styling - Clean Enterprise Design
# ══════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');

    * {
        font-family: 'Outfit', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    html, body, [class*="css"] {
        scroll-behavior: auto;
    }

    .stApp {
        background: linear-gradient(135deg, #FFFFFF 0%, #F9FAFB 100%);
    }

    /* Hide default streamlit elements */
    #MainMenu, footer, header { 
        visibility: hidden; 
    }

    /* Header */
    .header-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 1.25rem 2rem;
        border-bottom: 1px solid #E5E7EB;
        background: white;
        margin-bottom: 2rem;
    }

    .logo-section {
        display: flex;
        align-items: center;
        gap: 1rem;
    }

    .logo-img {
        height: 45px;
        object-fit: contain;
    }

    .title-section {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
    }

    .main-title {
        font-size: 1.25rem;
        font-weight: 700;
        color: #1F2937;
        margin: 0;
    }

    .sub-title {
        font-size: 0.7rem;
        color: #6B7280;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin: 0;
    }

    .format-selector-box {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        background: #F3F4F6;
        padding: 0.75rem 1rem;
        border-radius: 8px;
        border: 1px solid #D1D5DB;
    }

    .format-label {
        font-size: 0.7rem;
        color: #6B7280;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin: 0;
        white-space: nowrap;
    }

    /* Main Content Area */
    .main-container {
        padding: 0 2rem 2rem 2rem;
        max-width: 1000px;
        margin: 0 auto;
    }

    /* Upload Box */
    .upload-box {
        background: #F3F4F6;
        border: 2px dashed #D1D5DB;
        border-radius: 12px;
        padding: 3rem 2rem;
        text-align: center;
        transition: all 0.3s ease;
        margin-bottom: 2rem;
        min-height: 280px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
    }

    .upload-box:hover {
        border-color: #3B82F6;
        background: #F0F9FF;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.1);
    }

    .upload-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: #1F2937;
        margin-bottom: 0.75rem;
    }

    .upload-description {
        font-size: 0.875rem;
        color: #6B7280;
        margin-bottom: 1rem;
        line-height: 1.5;
    }

    .upload-info {
        font-size: 0.75rem;
        color: #9CA3AF;
        font-weight: 500;
    }

    /* Results Box */
    .results-box {
        background: linear-gradient(135deg, #F0F9FF 0%, #E0F2FE 100%);
        border: 1px solid #BAE6FD;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }

    .results-title {
        font-size: 1rem;
        font-weight: 700;
        color: #0C4A6E;
        margin-bottom: 1rem;
        margin-top: 0;
    }

    .metrics-row {
        display: flex;
        gap: 1.5rem;
        margin-bottom: 1rem;
    }

    .metric-item {
        flex: 1;
        background: white;
        padding: 0.75rem 1rem;
        border-radius: 8px;
        border: 1px solid #E5E7EB;
        text-align: center;
    }

    .metric-label {
        font-size: 0.75rem;
        color: #6B7280;
        font-weight: 600;
        text-transform: uppercase;
        margin-bottom: 0.5rem;
    }

    .metric-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: #1F2937;
    }

    /* Buttons */
    .stButton > button, .stDownloadButton > button {
        background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.75rem 1.5rem !important;
        font-weight: 600 !important;
        font-size: 0.875rem !important;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.25) !important;
        transition: all 0.2s ease !important;
        width: 100% !important;
    }

    .stButton > button:hover, .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #2563EB 0%, #1D4ED8 100%) !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 16px rgba(59, 130, 246, 0.35) !important;
    }

    /* Info boxes */
    .stInfo {
        background-color: #F0F9FF !important;
        border-left: 4px solid #0EA5E9 !important;
        padding: 0.75rem 1rem !important;
        border-radius: 8px !important;
        font-size: 0.875rem !important;
    }

    .stError {
        background-color: #FEF2F2 !important;
        border-left: 4px solid #EF4444 !important;
        padding: 0.75rem 1rem !important;
        border-radius: 8px !important;
    }

    /* Select dropdown styling */
    .stSelectbox > div > div {
        background: white !important;
        border: 1px solid #D1D5DB !important;
        border-radius: 8px !important;
    }

    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(90deg, #3B82F6 0%, #2563EB 100%);
    }

    /* Footer */
    .footer-section {
        text-align: center;
        color: #9CA3AF;
        font-size: 0.75rem;
        padding: 1.5rem 2rem;
        border-top: 1px solid #E5E7EB;
        margin-top: 2rem;
    }

    /* File uploader custom styling */
    [data-testid="stFileUploadDropzone"] {
        padding: 0 !important;
        border: none !important;
        background: transparent !important;
    }

    .stFileUploader {
        margin-bottom: 0 !important;
    }

    [data-testid="fileUploadDropzone"] label {
        cursor: pointer;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ══════════════════════════════════════════════════════════════════════════
#  Header Section
# ══════════════════════════════════════════════════════════════════════════

logo_b64 = get_base64_image("bajaj-life-logo.png")

header_col1, header_col2 = st.columns([3, 1])

with header_col1:
    st.markdown(
        f"""
        <div class="header-container">
            <div class="logo-section">
                <img src="data:image/png;base64,{logo_b64}" class="logo-img">
                <div class="title-section">
                    <div class="main-title">PDF to Excel</div>
                    <div class="sub-title">Table Extraction Engine</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with header_col2:
    st.markdown(
        f"""
        <div class="header-container" style="justify-content: flex-end; border: none;">
            <div class="format-selector-box">
                <span class="format-label">Format:</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    selected_format = st.selectbox(
        "Output Format",
        options=["excel", "csv", "json", "markdown"],
        format_func=lambda x: {
            "excel": "Excel (XLSX)",
            "csv": "CSV (ZIP)",
            "json": "JSON",
            "markdown": "Markdown"
        }[x],
        label_visibility="collapsed",
        key="format_select",
    )


# ══════════════════════════════════════════════════════════════════════════
#  Main Content
# ══════════════════════════════════════════════════════════════════════════

st.markdown('<div class="main-container">', unsafe_allow_html=True)

# File Upload Section
uploaded_file = st.file_uploader(
    "Upload PDF file",
    type=["pdf"],
    label_visibility="collapsed",
)

if not uploaded_file:
    st.markdown(
        """
        <div class="upload-box">
            <div class="upload-title">Click or Drag PDF here</div>
            <div class="upload-description">
                Drop your PDF file to begin extraction. Supports financial statements,
                invoices, reports, and complex tabular documents.
            </div>
            <div class="upload-info">
                Limit 200MB per file • PDF only • All 3 extraction methods applied
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# Track uploaded file
if uploaded_file:
    if 'last_uploaded' not in st.session_state or st.session_state.last_uploaded != uploaded_file.name:
        st.session_state.is_processed = False
        st.session_state.last_uploaded = uploaded_file.name

# File selected but not processed
if uploaded_file and not st.session_state.is_processed:
    st.markdown(
        f"""
        <div style='background: #F0F9FF; border-left: 4px solid #0EA5E9; padding: 1rem; border-radius: 8px; margin-bottom: 1.5rem;'>
            <div style='font-size: 0.875rem; color: #0C4A6E; font-weight: 600;'>
                Selected File: <strong>{uploaded_file.name}</strong> ({uploaded_file.size / (1024*1024):.1f} MB)
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    if st.button("Extract Tables", use_container_width=True):
        progress_bar = st.progress(0)
        status_text = st.empty()
        start_time = time.time()
        
        pdf_bytes = uploaded_file.getvalue()
        pdf_path = UPLOAD_DIR / uploaded_file.name
        pdf_path.write_bytes(pdf_bytes)
        
        try:
            def update_progress(current, total):
                pct = int((current / total) * 70)
                progress_bar.progress(pct)
                status_text.markdown(
                    f"<div style='text-align: center; color: #0EA5E9; font-size: 0.875rem; font-weight: 600;'>Processing page {current} of {total}...</div>",
                    unsafe_allow_html=True
                )

            extractor = PDFExtractor(pdf_path)
            result = extractor.extract(progress_callback=update_progress)
            
            progress_bar.progress(85)
            status_text.markdown(
                "<div style='text-align: center; color: #0EA5E9; font-size: 0.875rem; font-weight: 600;'>Reconstructing structure...</div>",
                unsafe_allow_html=True
            )
            
            if result.tables:
                reconstructor = TableReconstructor()
                result.tables = reconstructor.reconstruct(result.tables)
                
                if result.tables:
                    progress_bar.progress(95)
                    
                    ext_map = {
                        "excel": ".xlsx",
                        "csv": ".zip",
                        "json": ".json",
                        "markdown": ".md"
                    }
                    output_ext = ext_map.get(selected_format, ".xlsx")
                    output_path = OUTPUT_DIR / (pdf_path.stem + output_ext)
                    
                    formatter = OutputFormatter()
                    output_path = formatter.write_format(result, output_path, selected_format)
                    
                    st.session_state.extraction_result = result
                    st.session_state.output_path = output_path
                    st.session_state.output_format = selected_format
                    st.session_state.is_processed = True
                    st.session_state.elapsed_time = time.time() - start_time
                    
            progress_bar.progress(100)
            time.sleep(0.5)
            progress_bar.empty()
            status_text.empty()
            st.rerun()
            
        except Exception as exc:
            progress_bar.empty()
            status_text.empty()
            st.error(f"Extraction failed: {exc}")
            logger.exception("Extraction failed")
        finally:
            if pdf_path.exists():
                pdf_path.unlink()

# Show results if processed
if st.session_state.is_processed and st.session_state.extraction_result:
    result = st.session_state.extraction_result
    output_path = st.session_state.output_path
    output_format = st.session_state.get('output_format', 'excel')
    
    st.markdown('<div class="results-box">', unsafe_allow_html=True)
    st.markdown('<h3 class="results-title">Extraction Complete</h3>', unsafe_allow_html=True)
    
    m1, m2, m3 = st.columns(3)
    with m1:
        tables_count = len([t for t in result.tables if not t.is_empty])
        st.markdown(
            f"""
            <div class="metric-item">
                <div class="metric-label">Tables Found</div>
                <div class="metric-value">{tables_count}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with m2:
        rows = sum(t.total_rows for t in result.tables if not t.is_empty)
        st.markdown(
            f"""
            <div class="metric-item">
                <div class="metric-label">Total Rows</div>
                <div class="metric-value">{rows}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with m3:
        if any(not t.is_empty for t in result.tables):
            acc = sum(t.confidence for t in result.tables if not t.is_empty) / len([t for t in result.tables if not t.is_empty]) * 100
            acc_text = f"{acc:.0f}%"
        else:
            acc_text = "N/A"
        st.markdown(
            f"""
            <div class="metric-item">
                <div class="metric-label">Accuracy</div>
                <div class="metric-value">{acc_text}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    
    if output_path and output_path.exists():
        mime_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "csv": "application/zip",
            "json": "application/json",
            "markdown": "text/markdown"
        }
        mime_type = mime_types.get(output_format, "application/octet-stream")
        
        with open(output_path, "rb") as f:
            btn_data = f.read()
        
        st.download_button(
            label=f"Download {output_path.name}",
            data=btn_data,
            file_name=output_path.name,
            mime=mime_type,
            use_container_width=True,
        )
        
        if output_format == "csv":
            st.info("Multiple CSV files packaged in ZIP - extract to access individual tables")
        elif output_format == "json":
            st.info("Structured JSON format - preserves all extraction metadata and table details")
        elif output_format == "markdown":
            st.info("Markdown format - readable in any text editor")
        
        st.markdown(
            f"""
            <div style='text-align: center; color: #6B7280; font-size: 0.75rem; margin-top: 1rem;'>
                Extracted using hybrid method combining all 3 extraction engines • 
                Completed in {st.session_state.elapsed_time:.1f}s
            </div>
            """,
            unsafe_allow_html=True
        )
    
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Process Another File", use_container_width=True):
        st.session_state.is_processed = False
        st.session_state.extraction_result = None
        st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
#  Footer
# ══════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <div class="footer-section">
        © 2026 Bajaj Life Insurance • Gen AI Table Extraction Engine • Internal Use Only
    </div>
    """,
    unsafe_allow_html=True,
)
