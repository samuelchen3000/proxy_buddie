#!/usr/bin/env python3
"""
Script to process extracted_tables.csv and replicate the structure found in extracted_tables folder.
This script creates individual table CSV files, combined tables, and metadata JSON.
"""

import pandas as pd
import json
import re
import os
from typing import Dict, List, Any, Tuple
import csv

class TableExtractionProcessor:
    def __init__(self, input_csv_path: str, output_dir: str = "extracted_tables"):
        self.input_csv_path = input_csv_path
        self.output_dir = output_dir
        self.tables_metadata = []
        self.table_counter = 1
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def parse_markdown_table(self, markdown_text: str) -> Tuple[List[str], List[List[str]]]:
        """Parse markdown table format and extract headers and rows."""
        if not markdown_text or not markdown_text.strip():
            return [], []
        
        lines = markdown_text.strip().split('\n')
        headers = []
        rows = []
        separator_pattern = re.compile(r'^\s*[\|\-\s:]+\s*$')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line or not line.startswith('|'):
                continue
            
            # Skip separator rows (e.g., |---|---|)
            if separator_pattern.match(line):
                continue
            
            # Remove leading and trailing pipes
            cells = [cell.strip() for cell in line.split('|')[1:-1]]
            
            # Skip empty rows (all cells are empty or whitespace)
            if all(not cell or cell == '-' or cell.isspace() for cell in cells):
                continue
            
            if not headers:
                headers = cells
            else:
                rows.append(cells)
        
        return headers, rows
    
    def clean_column_name(self, col_name: str) -> str:
        """Clean and normalize column names."""
        if not col_name:
            return f"column_{len(self.current_headers)}"
        
        # Remove special characters and replace with underscores
        cleaned = re.sub(r'[^\w\s]', '_', col_name)
        cleaned = re.sub(r'\s+', '_', cleaned)
        cleaned = re.sub(r'_+', '_', cleaned)
        cleaned = cleaned.strip('_')
        
        return cleaned.lower() if cleaned else f"column_{len(self.current_headers)}"
    
    def extract_footnotes(self, footnotes_text: str) -> List[str]:
        """Extract footnotes from text."""
        if not footnotes_text or footnotes_text.strip() == '':
            return []
        
        # Split by common footnote patterns
        footnotes = []
        lines = footnotes_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if line and (line.startswith('-') or line.startswith('(') or line.startswith('*')):
                footnotes.append(line)
        
        return footnotes if footnotes else [footnotes_text] if footnotes_text.strip() else []
    
    def determine_table_type(self, caption: str, headers: List[str]) -> str:
        """Determine table type based on caption and headers."""
        caption_lower = caption.lower() if caption and isinstance(caption, str) else ""
        headers_lower = ' '.join([str(h) for h in headers]).lower() if headers else ""
        
        if 'executive' in caption_lower and 'compensation' in caption_lower:
            return 'executive_compensation'
        elif 'director' in caption_lower and 'compensation' in caption_lower:
            return 'director_compensation'
        elif 'equity' in caption_lower and 'award' in caption_lower:
            return 'equity_awards'
        elif 'ownership' in caption_lower or 'beneficially' in headers_lower:
            return 'ownership'
        elif 'grant' in caption_lower and 'award' in caption_lower:
            return 'grants_awards'
        else:
            return 'unknown'
    
    def process_table_row(self, row: pd.Series) -> None:
        """Process a single row from the CSV that contains table data."""
        try:
            section_index = row['section_index']
            header_norm = row['header_norm'] if pd.notna(row['header_norm']) else ""
            caption = row['table_caption_or_context'] if pd.notna(row['table_caption_or_context']) else ""
            markdown = row['table_markdown'] if pd.notna(row['table_markdown']) else ""
            legends = row['table_legends'] if pd.notna(row['table_legends']) else ""
            footnotes = row['table_footnotes'] if pd.notna(row['table_footnotes']) else ""
            first_header_cells = row['first_header_cells'] if pd.notna(row['first_header_cells']) else ""
        except Exception as e:
            print(f"Error extracting row data: {e}")
            return
        
        # Skip if no markdown table data
        if not markdown or not markdown.strip():
            return
        
        # Parse the markdown table
        headers, rows = self.parse_markdown_table(markdown)
        
        if not headers or not rows:
            return
        
        # Clean headers
        self.current_headers = headers
        cleaned_headers = [self.clean_column_name(h) for h in headers]
        
        # Determine table type
        table_type = self.determine_table_type(caption, headers)
        
        # Create table ID
        table_id = f"2_{self.table_counter:02d}"
        
        # Extract footnotes
        footnote_list = self.extract_footnotes(footnotes)
        
        # Create metadata entry
        metadata = {
            "table_id": table_id,
            "section_index": section_index,
            "table_type": table_type,
            "header_norm": header_norm,
            "table_caption": caption,
            "footnotes": footnote_list,
            "legends": [legends] if legends else [],
            "first_header_cells": headers,
            "num_rows": len(rows),
            "num_columns": len(headers),
            "columns": headers
        }
        
        self.tables_metadata.append(metadata)
        
        # Create individual table CSV
        self.create_individual_table_csv(table_id, table_type, headers, rows, caption, footnote_list)
        
        # Create combined table entry
        self.create_combined_table_entry(table_id, section_index, header_norm, caption, 
                                       table_type, headers, rows)
        
        self.table_counter += 1
    
    def create_individual_table_csv(self, table_id: str, table_type: str, 
                                  headers: List[str], rows: List[List[str]], 
                                  caption: str, footnotes: List[str]) -> None:
        """Create individual table CSV file."""
        filename = f"table_{table_id}_{table_type}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write headers
            writer.writerow(headers)
            
            # Write data rows
            for row in rows:
                writer.writerow(row)
            
            # Add empty row
            writer.writerow([])
            
            # Add footnotes section
            if footnotes:
                writer.writerow(['FOOTNOTES:'] + [''] * (len(headers) - 1))
                for footnote in footnotes:
                    writer.writerow([footnote] + [''] * (len(headers) - 1))
    
    def create_combined_table_entry(self, table_id: str, section_index: int, 
                                  header_norm: str, caption: str, table_type: str,
                                  headers: List[str], rows: List[List[str]]) -> None:
        """Create combined table CSV entry."""
        filename = f"combined_table_{self.table_counter:02d}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # Create column names with table prefix
        prefixed_headers = [f"table_{self.table_counter:02d}_{self.clean_column_name(h)}" 
                          for h in headers]
        
        # Add metadata columns
        all_headers = prefixed_headers + ['table_id', 'table_section_index', 
                                        'table_header', 'table_caption', 'table_type']
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write headers
            writer.writerow(all_headers)
            
            # Write data rows with metadata
            for row in rows:
                metadata_row = [table_id, section_index, header_norm, caption, table_type]
                writer.writerow(row + metadata_row)
    
    def save_metadata(self) -> None:
        """Save table metadata to JSON file."""
        metadata_filepath = os.path.join(self.output_dir, 'table_metadata.json')
        with open(metadata_filepath, 'w', encoding='utf-8') as f:
            json.dump(self.tables_metadata, f, indent=2, ensure_ascii=False)
    
    def process(self) -> None:
        """Main processing function."""
        print(f"Processing {self.input_csv_path}...")
        
        # Read the CSV file
        df = pd.read_csv(self.input_csv_path)
        
        print(f"Found {len(df)} rows to process")
        
        # Process each row
        for index, row in df.iterrows():
            try:
                self.process_table_row(row)
                if index % 10 == 0:
                    print(f"Processed {index + 1} rows...")
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                continue
        
        # Save metadata
        self.save_metadata()
        
        print(f"Processing complete!")
        print(f"Created {len(self.tables_metadata)} tables")
        print(f"Output directory: {self.output_dir}")

def main():
    """Main function."""
    input_file = "universal_tables_tables_only (6).csv"
    output_dir = "extracted_tables"
    
    processor = TableExtractionProcessor(input_file, output_dir)
    processor.process()

if __name__ == "__main__":
    main()
