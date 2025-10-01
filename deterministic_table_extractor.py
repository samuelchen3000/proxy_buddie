#!/usr/bin/env python3
"""
Deterministic Table Extractor

This script extracts tables from Docling output text files in a deterministic manner.
It processes markdown-style tables and extracts them with their context, captions, legends, and footnotes.

Features:
- Extracts markdown-style tables (| ... |)
- Harvests captions and context above tables
- Extracts legends and footnotes below tables
- Deduplicates tables based on content
- Exports results to CSV format
"""

import re
import csv
import hashlib
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DeterministicTableExtractor:
    """Extract tables from Docling output text files deterministically."""
    
    def __init__(self, input_path: str, output_path: str = "extracted_tables"):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True)
        
        # Regex patterns for table detection
        self.TABLE_SEP_RE = re.compile(r'^\s*\|[\s\-\|:]+\|\s*$')
        self.LEGEND_RE = re.compile(r"^\s*[A-Z]{2,}\s*[-–—]\s+.+$")
        self.CAPTION_RE = re.compile(r"\btable\b", re.IGNORECASE)
        self.FOOTNOTE_START_RE = re.compile(
            r"""^\s*                                   # optional leading spaces
                (?:[-•*]\s*)?                          # optional bullet: -, •, *
                \(\s*(?:\d+|[A-Za-z]+|[IVXLCDM]+)\s*\) # (1) or (a/A) or (iv/IV)
                [\s.:;-]+                               # required delimiter after marker
            """,
            re.VERBOSE
        )
        
        # Structural headings to ignore
        self.STRUCTURAL_HEADINGS = {
            "back to contents",
            "table of contents",
            "alphabet 2025 proxy statement",
        }
    
    def _is_row_like(self, line: str) -> bool:
        """Check if a line looks like a table row."""
        if not line.strip():
            return False
        
        # Must contain at least one pipe
        if '|' not in line:
            return False
        
        # Count pipes - should be at least 2 for a valid row
        pipe_count = line.count('|')
        if pipe_count < 2:
            return False
        
        # Check for reasonable cell content (not just separators)
        cells = [cell.strip() for cell in line.split('|')]
        non_empty_cells = [cell for cell in cells if cell and not re.match(r'^[\s\-:]+$', cell)]
        
        return len(non_empty_cells) >= 2
    
    def extract_all_tables(self, content: str) -> List[str]:
        """Extract contiguous row-like/table-separator blocks."""
        lines = [ln.rstrip() for ln in content.splitlines()]
        blocks, cur = [], []
        
        for ln in lines:
            if self._is_row_like(ln) or self.TABLE_SEP_RE.match(ln):
                cur.append(ln)
            else:
                if cur:
                    blocks.append("\n".join(cur).strip())
                    cur = []
        
        if cur:
            blocks.append("\n".join(cur).strip())
        
        return [b for b in blocks if b.count("|") >= 4]
    
    def extract_legends_near_tables(self, content: str, window: int = 12) -> List[str]:
        """Collect lines matching legend pattern within a window around any table."""
        lines = content.splitlines()
        
        # Mark table line indices
        table_line_idx = set()
        for i, ln in enumerate(lines):
            if self._is_row_like(ln) or self.TABLE_SEP_RE.match(ln):
                table_line_idx.add(i)
        
        if not table_line_idx:
            return []
        
        lo = max(0, min(table_line_idx) - window)
        hi = min(len(lines), max(table_line_idx) + window + 1)
        
        legends = [ln.strip() for ln in lines[lo:hi] if self.LEGEND_RE.match(ln)]
        
        # Deduplicate while preserving order
        seen, out = set(), []
        for x in legends:
            if x not in seen:
                seen.add(x)
                out.append(x)
        
        return out
    
    def extract_caption_near_table(self, content: str, window: int = 4) -> str:
        """Extract caption near table."""
        lines = content.splitlines()
        
        # Find first table block start line
        first_tbl = None
        for i, ln in enumerate(lines):
            if self._is_row_like(ln) or self.TABLE_SEP_RE.match(ln):
                first_tbl = i
                break
        
        if first_tbl is None:
            return ""
        
        lo = max(0, first_tbl - window)
        hi = min(len(lines), first_tbl + 1)
        
        candidates = [ln.strip() for ln in lines[lo:hi] if self.CAPTION_RE.search(ln)]
        return candidates[0] if candidates else ""
    
    def extract_footnote_paragraphs(self, text: str) -> List[str]:
        """Extract footnotes and handle multi-line continuation."""
        lines = text.splitlines()
        notes, buf, in_note = [], [], False
        
        def flush():
            nonlocal buf
            if buf:
                notes.append("\n".join(buf).strip())
                buf = []
        
        i = 0
        while i < len(lines):
            ln = lines[i]
            if self.FOOTNOTE_START_RE.match(ln):
                if in_note:
                    flush()
                in_note = True
                buf = [ln]
                i += 1
                
                while i < len(lines):
                    nxt = lines[i]
                    if not nxt.strip():  # blank line ends the note
                        buf.append(nxt)
                        i += 1
                        break
                    elif self.FOOTNOTE_START_RE.match(nxt):  # another footnote starts
                        break
                    else:
                        buf.append(nxt)
                        i += 1
                
                if i >= len(lines):
                    flush()
            else:
                if in_note:
                    flush()
                    in_note = False
                i += 1
        
        if in_note:
            flush()
        
        return notes
    
    def extract_context_above_table(self, content: str, window: int = 4) -> str:
        """Extract context lines above the first table."""
        lines = content.splitlines()
        
        # Find first table line
        first_tbl = None
        for i, ln in enumerate(lines):
            if self._is_row_like(ln) or self.TABLE_SEP_RE.match(ln):
                first_tbl = i
                break
        
        if first_tbl is None:
            return ""
        
        lo = max(0, first_tbl - window)
        context_lines = [ln.strip() for ln in lines[lo:first_tbl] if ln.strip()]
        return " ".join(context_lines)
    
    def extract_first_header_cells(self, table_markdown: str) -> str:
        """Extract first header cells from table markdown."""
        lines = table_markdown.splitlines()
        if not lines:
            return ""
        
        first_line = lines[0]
        cells = [cell.strip() for cell in first_line.split('|')]
        # Remove empty cells at start/end
        while cells and not cells[0]:
            cells.pop(0)
        while cells and not cells[-1]:
            cells.pop()
        
        return " | ".join(cells[:3])  # First 3 cells
    
    def normalize_table_md_for_dedupe(self, md: str) -> str:
        """Normalize table markdown for deduplication."""
        if not md:
            return ""
        
        # Normalize line endings
        md = md.replace("\r\n", "\n").strip()
        
        # Collapse runs of spaces/tabs inside lines
        md = "\n".join(re.sub(r"[ \t]+", " ", ln.strip()) for ln in md.split("\n"))
        
        # Collapse multiple blank lines
        md = re.sub(r"\n{3,}", "\n\n", md)
        
        return md
    
    def uniq_join(self, values):
        """Join unique non-empty strings in order, separated by double newlines."""
        seq = [v for v in values if isinstance(v, str) and v.strip()]
        seen, out = set(), []
        for v in seq:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return "\n\n".join(out)
    
    def split_into_sections(self, content: str) -> List[Dict]:
        """Split content into sections based on headings."""
        lines = content.splitlines()
        sections = []
        current_section = {"index": 0, "header_raw": "", "header_norm": "", "content": ""}
        
        for line in lines:
            # Check if this is a section header (starts with ##)
            if line.strip().startswith("## "):
                # Save previous section
                if current_section["content"].strip():
                    sections.append(current_section)
                
                # Start new section
                header = line.strip()[3:].strip()  # Remove "## "
                current_section = {
                    "index": len(sections),
                    "header_raw": header,
                    "header_norm": header.lower().strip(),
                    "content": ""
                }
            else:
                current_section["content"] += line + "\n"
        
        # Add final section
        if current_section["content"].strip():
            sections.append(current_section)
        
        return sections
    
    def process_section(self, section: Dict) -> List[Dict]:
        """Process a single section to extract table information."""
        content = section["content"]
        tables = self.extract_all_tables(content)
        
        if not tables:
            return []
        
        # Extract context information
        legends = self.extract_legends_near_tables(content)
        caption = self.extract_caption_near_table(content)
        footnotes = self.extract_footnote_paragraphs(content)
        context_above = self.extract_context_above_table(content)
        
        table_info = []
        for table_md in tables:
            first_header_cells = self.extract_first_header_cells(table_md)
            
            table_info.append({
                "table_markdown": table_md,
                "legends": "\n".join(legends),
                "footnotes": "\n".join(footnotes),
                "caption": caption,
                "context_above": context_above,
                "first_header_cells": first_header_cells
            })
        
        return table_info
    
    def extract_tables(self) -> pd.DataFrame:
        """Main extraction method."""
        logger.info(f"Reading input file: {self.input_path}")
        
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_path}")
        
        with open(self.input_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split into sections
        sections = self.split_into_sections(content)
        logger.info(f"Found {len(sections)} sections")
        
        # Process each section
        all_table_rows = []
        for section in sections:
            table_info_list = self.process_section(section)
            
            for table_info in table_info_list:
                all_table_rows.append({
                    "section_index": section["index"],
                    "header_norm": section["header_norm"],
                    "table_caption_or_context": table_info["caption"] or table_info["context_above"],
                    "table_markdown": table_info["table_markdown"],
                    "table_legends": table_info["legends"],
                    "table_footnotes": table_info["footnotes"],
                    "first_header_cells": table_info["first_header_cells"],
                })
        
        logger.info(f"Extracted {len(all_table_rows)} table instances")
        
        # Create DataFrame
        df = pd.DataFrame(all_table_rows)
        
        if df.empty:
            logger.warning("No tables found in the input file")
            return df
        
        # Deduplicate tables
        df["__md_norm__"] = df["table_markdown"].map(self.normalize_table_md_for_dedupe)
        
        agg_df = (
            df
            .groupby("__md_norm__", as_index=False)
            .agg({
                "section_index": "min",
                "header_norm": "first",
                "table_caption_or_context": self.uniq_join,
                "table_markdown": "first",
                "table_legends": self.uniq_join,
                "table_footnotes": self.uniq_join,
                "first_header_cells": "first"
            })
            .drop(columns=["__md_norm__"])
        )
        
        logger.info(f"After deduplication: {len(agg_df)} unique tables")
        
        return agg_df
    
    def save_results(self, df: pd.DataFrame, output_filename: str = "extracted_tables.csv"):
        """Save results to CSV file."""
        output_file = self.output_path / output_filename
        df.to_csv(output_file, index=False, quoting=csv.QUOTE_MINIMAL)
        logger.info(f"Saved results to: {output_file}")
        return output_file
    
    def run(self, output_filename: str = "extracted_tables.csv") -> Path:
        """Run the complete extraction process."""
        logger.info("Starting deterministic table extraction...")
        
        df = self.extract_tables()
        output_file = self.save_results(df, output_filename)
        
        logger.info("Table extraction completed successfully")
        return output_file

def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract tables from Docling output deterministically')
    parser.add_argument('--input', required=True, help='Input text file path')
    parser.add_argument('--output-dir', default='extracted_tables', help='Output directory')
    parser.add_argument('--output-file', default='extracted_tables.csv', help='Output CSV filename')
    
    args = parser.parse_args()
    
    extractor = DeterministicTableExtractor(args.input, args.output_dir)
    output_file = extractor.run(args.output_file)
    
    print(f"\nTable extraction completed!")
    print(f"Results saved to: {output_file}")
    print(f"Total tables extracted: {len(pd.read_csv(output_file))}")

if __name__ == "__main__":
    main()
