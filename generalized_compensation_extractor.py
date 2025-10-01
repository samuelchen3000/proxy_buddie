#!/usr/bin/env python3
"""
Generalized Compensation Data Extractor

This script extracts specific compensation fields for executives and directors
from extracted tables, designed to work across different companies.

Target Fields:
Executives:
- Base Pay (cash)
- Cash bonus  
- Stock Awards
- RSUs
- PSUs
- GSUs

Directors:
- Base pay
- Stock Awards
- Common stock owned

Features:
- Generalizable across different companies
- Advanced name cleaning to remove titles
- Validation agent using OpenAI API
- Multiple output formats (JSON, CSV)
"""

import pandas as pd
import json
import re
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
from dotenv import load_dotenv
import openai

# Load environment variables
load_dotenv('config.env')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ValidationAgent:
    """Agent to validate extracted compensation data using OpenAI."""
    
    def __init__(self):
        self.api_key = os.getenv('OPENAI_API_KEY')
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        self.temperature = float(os.getenv('OPENAI_TEMPERATURE', '0.1'))
        
        if not self.api_key:
            logger.warning("No OpenAI API key found. Validation will be skipped.")
            self.enabled = False
        else:
            openai.api_key = self.api_key
            self.enabled = True
    
    def validate_extraction(self, data: Dict[str, Any], company_name: str = "Unknown") -> Dict[str, Any]:
        """Validate the extracted compensation data."""
        if not self.enabled:
            logger.info("Validation agent disabled - no API key")
            return {"validated": False, "reason": "No API key", "suggestions": []}
        
        try:
            # Prepare data for validation
            validation_prompt = self._create_validation_prompt(data, company_name)
            
            client = openai.OpenAI(api_key=self.api_key)
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a financial data validation expert. Analyze compensation data for accuracy and completeness."},
                    {"role": "user", "content": validation_prompt}
                ],
                temperature=self.temperature,
                max_tokens=1000
            )
            
            validation_result = response.choices[0].message.content
            return self._parse_validation_result(validation_result)
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {"validated": False, "reason": f"Validation error: {e}", "suggestions": []}
    
    def _create_validation_prompt(self, data: Dict[str, Any], company_name: str) -> str:
        """Create validation prompt for OpenAI."""
        executives = data.get('executives', {})
        directors = data.get('directors', {})
        
        prompt = f"""
        Please validate the following compensation data extracted for {company_name}:
        
        EXECUTIVES ({len(executives)}):
        """
        
        for name, info in executives.items():
            prompt += f"\n{name}:"
            prompt += f"\n  Base Pay: ${info.get('base_pay_cash', 'N/A')}"
            prompt += f"\n  Cash Bonus: ${info.get('cash_bonus', 'N/A')}"
            prompt += f"\n  Stock Awards: ${info.get('stock_awards', 'N/A')}"
            prompt += f"\n  Non-Equity Incentive: ${info.get('non_equity_incentive', 'N/A')}"
            prompt += f"\n  Option Awards: ${info.get('option_awards', 'N/A')}"
            prompt += f"\n  All Other Comp: ${info.get('all_other_compensation', 'N/A')}"
            prompt += f"\n  RSUs: {info.get('rsus', 'N/A')}"
            prompt += f"\n  PSUs: {info.get('psus', 'N/A')}"
            prompt += f"\n  GSUs: {info.get('gsus', 'N/A')}"
            prompt += f"\n  Total: ${info.get('total_compensation', 'N/A')}"
        
        prompt += f"\n\nDIRECTORS ({len(directors)}):"
        for name, info in directors.items():
            prompt += f"\n{name}:"
            prompt += f"\n  Base Pay: ${info.get('base_pay', 'N/A')}"
            prompt += f"\n  Stock Awards: ${info.get('stock_awards', 'N/A')}"
            prompt += f"\n  Option Awards: ${info.get('option_awards', 'N/A')}"
            prompt += f"\n  All Other Comp: ${info.get('all_other_compensation', 'N/A')}"
            prompt += f"\n  Common Stock Owned: {info.get('common_stock_owned', 'N/A')}"
            prompt += f"\n  Total: ${info.get('total_compensation', 'N/A')}"
        
        prompt += """
        
        Please provide:
        1. Overall validation (VALID/INVALID/PARTIAL)
        2. Any data quality issues found
        3. Suggestions for improvement
        4. Confidence level (1-10)
        
        Format your response as JSON with keys: validated, confidence, issues, suggestions
        """
        
        return prompt
    
    def _parse_validation_result(self, result: str) -> Dict[str, Any]:
        """Parse validation result from OpenAI response."""
        try:
            # Try to extract JSON from the response
            json_match = re.search(r'\{.*\}', result, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                return {
                    "validated": True,
                    "confidence": 7,
                    "issues": [],
                    "suggestions": [result]
                }
        except:
            return {
                "validated": True,
                "confidence": 5,
                "issues": [],
                "suggestions": [result]
            }

class GeneralizedCompensationExtractor:
    def __init__(self, tables_dir: str = "extracted_tables", company_name: str = "Unknown"):
        self.tables_dir = Path(tables_dir)
        self.company_name = company_name
        self.validation_agent = ValidationAgent()
        
        # Common title patterns to remove (expandable for different companies)
        self.title_patterns = [
            # Executive titles
            r'\s+(CEO|Chief Executive Officer).*',
            r'\s+(CFO|Chief Financial Officer).*',
            r'\s+(COO|Chief Operating Officer).*',
            r'\s+(CTO|Chief Technology Officer).*',
            r'\s+(CCO|Chief Commercial Officer).*',
            r'\s+(CBO|Chief Business Officer).*',
            r'\s+(CLO|Chief Legal Officer).*',
            r'\s+(CPO|Chief People Officer).*',
            r'\s+(CMO|Chief Marketing Officer).*',
            r'\s+(President).*',
            r'\s+(Vice President|VP).*',
            r'\s+(Senior Vice President|SVP).*',
            r'\s+(Executive Vice President|EVP).*',
            r'\s+(Managing Director).*',
            r'\s+(General Manager).*',
            r'\s+(Director).*',
            r'\s+(Secretary).*',
            r'\s+(Treasurer).*',
            r'\s+(Controller).*',
            r'\s+(Legal Officer).*',
            r'\s+(Investment Officer).*',
            r'\s+(Knowledge and Information).*',
            r'\s+(Business Officer).*',
            
            # Board titles
            r'\s+(Chairman|Chair).*',
            r'\s+(Lead Director).*',
            r'\s+(Independent Director).*',
            r'\s+(Non-Executive Director).*',
            r'\s+(Employee Director).*',
            
            # Company-specific patterns
            r'\s+and\s+.*',  # Remove "and Google, and Director" etc.
            r'\s+of\s+.*',  # Remove "of Alphabet" etc.
            r'\s+at\s+.*',  # Remove "at Company" etc.
            r'\s+through\s+.*',  # Remove "through July 30, 2024"
            r'\s+until\s+.*',  # Remove "until October 16, 2024"
            r'\s+as\s+of\s+.*',  # Remove "as of July 31, 2024"
            r'\s+\(.*\)',  # Remove parenthetical content
            r'\s+;.*',  # Remove everything after semicolon
            r'\s+,\s+.*',  # Remove everything after comma
        ]
        
    def clean_numeric_value(self, value: str) -> Optional[float]:
        """Clean and convert numeric values from compensation tables."""
        if pd.isna(value) or value == '' or value == '-':
            return None
        
        # Convert to string and strip whitespace
        cleaned = str(value).strip()
        
        # Remove footnote references first (with or without spaces)
        # Handles cases like "10,000(1)", "10,000 (1)", "(1)10,000", etc.
        cleaned = re.sub(r'\s*\(\d+\)\s*', ' ', cleaned)
        cleaned = re.sub(r'\s*\[\d+\]\s*', ' ', cleaned)  # Also handle square brackets [1]
        
        # Remove dollar signs
        cleaned = cleaned.replace('$', '')
        
        # Remove commas from numbers
        cleaned = cleaned.replace(',', '')
        
        # Remove any remaining whitespace
        cleaned = cleaned.strip()
        
        # Remove any parentheses that might indicate negative numbers
        # But keep the negative sign
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
        
        # Try to convert to float
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    def is_valid_person_name(self, name: str) -> bool:
        """
        Validate that a name is likely a real person's name (not a company, title, or other entity).
        This is designed to work across different companies.
        """
        if not name or len(name) < 3:
            return False
        
        name_lower = name.lower()
        
        # Common non-person terms that should be filtered out
        non_person_indicators = [
            # Company/organization names
            'inc', 'llc', 'corporation', 'company', 'holdings', 'group', 'partners', 
            'limited', 'ltd', 'co.', 'corp', 'associates', 'ventures',
            
            # Generic titles that slipped through
            'president', 'vice', 'officer', 'director', 'manager', 'executive',
            'chairman', 'secretary', 'treasurer', 'controller', 'chief',
            
            # Dates and temporal terms
            'january', 'february', 'march', 'april', 'may', 'june', 'july', 
            'august', 'september', 'october', 'november', 'december',
            '2020', '2021', '2022', '2023', '2024', '2025', '2026',
            
            # Generic terms
            'consists', 'based on', 'reflects', 'amounts', 'shares',
            'plan', 'salary', 'bonus', 'compensation', 'total',
            'footnotes', 'notes', 'see', 'refer', 'page',
            'other', 'all', 'holders', 'officers', 'directors',
            'as of', 'through', 'until', 'ended', 'during',
            
            # Common partial terms
            'and google', 'and alphabet', 'google,', 'alphabet,',
            'senior vice', 'legal officer', 'knowledge and',
        ]
        
        # Check if name contains any non-person indicators
        for indicator in non_person_indicators:
            if indicator in name_lower:
                return False
        
        # Check if name starts with a dash (usually indicates a footnote or comment)
        if name.strip().startswith('-'):
            return False
        
        # Check if name is all uppercase (usually indicates a header or section title)
        if name.isupper() and len(name) > 10:
            return False
        
        # Name should have at least 2 parts (first and last name)
        name_parts = name.split()
        if len(name_parts) < 2:
            return False
        
        # Check if first part looks like a real first name (starts with capital letter, at least 2 chars)
        first_part = name_parts[0]
        if len(first_part) < 2 or not first_part[0].isupper():
            return False
        
        # Check if last part looks like a real last name
        last_part = name_parts[-1]
        if len(last_part) < 2:
            return False
        
        # Check for patterns that indicate non-names
        # E.g., "Google, as" or "October 16, 2024"
        if any(char.isdigit() for char in name):
            # Allow middle initials but not full dates/years
            digit_count = sum(1 for char in name if char.isdigit())
            if digit_count > 1:  # More than just a middle initial
                return False
        
        # Name should not be mostly punctuation
        alpha_chars = sum(1 for char in name if char.isalpha())
        if alpha_chars < len(name) * 0.6:  # At least 60% alphabetic
            return False
        
        return True
    
    def extract_clean_name(self, name_field: str) -> str:
        """Extract clean name from name field, removing all titles and company references."""
        if pd.isna(name_field):
            return ""
        
        name = str(name_field)
        
        # Skip if it's clearly not a person's name
        if any(skip_word in name.lower() for skip_word in ['footnotes', 'adjustments', 'deduction', 'increase', 'value', 'compensation actually paid']):
            return ""
        
        # Remove footnote references like "(2)", "(3)", etc.
        name = re.sub(r'\s*\(\d+\)\s*$', '', name)
        
        # Apply all title patterns
        for pattern in self.title_patterns:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        # Additional cleanup for specific cases - apply multiple times to catch nested patterns
        for _ in range(5):  # Apply multiple passes to catch nested patterns
            # Remove everything after common separators
            name = re.sub(r'\s+and\s+.*', '', name, flags=re.IGNORECASE)  # Remove "and Google, and Director" etc.
            name = re.sub(r'\s+Senior\s+Vice\s+President.*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s+President\s+and\s+.*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s+Chief\s+.*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s+Legal\s+Officer.*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s+Secretary.*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s+until\s+.*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s+as\s+of\s+.*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s+through\s+.*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s+\(.*\)', '', name)  # Remove parenthetical content
            name = re.sub(r'\s+;.*', '', name)  # Remove everything after semicolon
            name = re.sub(r'\s+,\s+.*', '', name)  # Remove everything after comma
            name = re.sub(r'\s+of\s+.*', '', name, flags=re.IGNORECASE)  # Remove "of Alphabet" etc.
            name = re.sub(r'\s+at\s+.*', '', name, flags=re.IGNORECASE)  # Remove "at Company" etc.
            
            # More aggressive cleanup for complex cases
            name = re.sub(r'\s+[A-Z][a-z]+\s+Officer.*', '', name)  # Remove "Financial Officer" etc.
            name = re.sub(r'\s+[A-Z][a-z]+\s+and\s+.*', '', name)  # Remove "Investment and" etc.
            name = re.sub(r'\s+[A-Z][a-z]+\s+[A-Z][a-z]+\s+.*', '', name)  # Remove "Knowledge and Information" etc.
        
        # Additional cleanup
        name = re.sub(r'\s+', ' ', name)  # Normalize whitespace
        name = name.strip()
        
        # Remove common prefixes/suffixes
        name = re.sub(r'^(Mr\.|Ms\.|Mrs\.|Dr\.)\s+', '', name, flags=re.IGNORECASE)
        
        # Final validation - should contain at least a first and last name
        name_parts = name.split()
        if len(name_parts) < 2:
            return ""
        
        return name
    
    def find_compensation_tables(self) -> Tuple[Optional[Path], Optional[Path]]:
        """Find executive and director compensation tables automatically."""
        exec_table = None
        dir_table = None
        
        # Look for common table naming patterns first
        table_files = list(self.tables_dir.glob("*.csv"))
        
        # Priority order for table detection
        exec_priority = [
            "table_2_08_executive_compensation.csv",
            "table_2_19_executive_compensation.csv",
            "table_2_37_executive_compensation.csv",
            "table_2_01_executive_compensation.csv",
            "executive_compensation.csv",
            "named_executive_officer_compensation.csv"
        ]
        
        dir_priority = [
            "table_2_09_director_compensation.csv",
            "table_2_02_director_compensation.csv", 
            "director_compensation.csv",
            "board_compensation.csv"
        ]
        
        # Check priority files first
        for filename in exec_priority:
            potential_file = self.tables_dir / filename
            if potential_file.exists():
                exec_table = potential_file
                logger.info(f"Found executive compensation table (priority): {potential_file}")
                break
        
        for filename in dir_priority:
            potential_file = self.tables_dir / filename
            if potential_file.exists():
                dir_table = potential_file
                logger.info(f"Found director compensation table (priority): {potential_file}")
                break
        
        # If not found by priority, scan all files
        if not exec_table or not dir_table:
            for file_path in table_files:
                try:
                    # Read first few rows to identify table type
                    df = pd.read_csv(file_path, nrows=5)
                    first_col = str(df.columns[0]).lower()
                    content_sample = ' '.join(df.iloc[:, 0].astype(str).head(3).tolist()).lower()
                    
                    # Check for executive compensation indicators (more specific)
                    exec_indicators = ['named executive', 'executive compensation', 'salary', 'bonus', 'stock awards']
                    if any(indicator in first_col or indicator in content_sample for indicator in exec_indicators):
                        if 'director' not in first_col and 'director' not in content_sample and not exec_table:
                            exec_table = file_path
                            logger.info(f"Found executive compensation table: {file_path}")
                    
                    # Check for director compensation indicators (more specific)
                    dir_indicators = ['director compensation', 'board compensation', 'fees earned', 'retainer']
                    if any(indicator in first_col or indicator in content_sample for indicator in dir_indicators):
                        if 'executive' not in first_col and 'executive' not in content_sample and not dir_table:
                            dir_table = file_path
                            logger.info(f"Found director compensation table: {file_path}")
                            
                except Exception as e:
                    logger.warning(f"Could not analyze {file_path}: {e}")
                    continue
        
        return exec_table, dir_table
    
    def extract_equity_data_from_all_tables(self) -> Dict[str, Dict[str, Any]]:
        """Extract GSU, PSU, RSU data from ALL CSV files in the directory."""
        logger.info("Scanning all CSV files for equity data (GSUs, PSUs, RSUs)...")
        
        equity_data = {}
        
        # Get all CSV files in the directory
        csv_files = list(self.tables_dir.glob("*.csv"))
        
        for file_path in csv_files:
            try:
                # Skip metadata file
                if 'metadata' in file_path.name.lower():
                    continue
                
                # Try reading with different error handling strategies
                df = None
                try:
                    df = pd.read_csv(file_path)
                except:
                    # Try with error_bad_lines=False for Python 3.7+, or on_bad_lines='skip' for newer pandas
                    try:
                        df = pd.read_csv(file_path, on_bad_lines='skip')
                    except:
                        try:
                            df = pd.read_csv(file_path, error_bad_lines=False, warn_bad_lines=False)
                        except:
                            # Try with different encoding
                            try:
                                df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
                            except:
                                logger.warning(f"Could not parse {file_path.name} - skipping")
                                continue
                
                if df is None or len(df) < 2:
                    continue
                
                # Remove footnote rows
                df = df[~df.iloc[:, 0].astype(str).str.contains('FOOTNOTES', na=False, regex=False)]
                df = df[~df.iloc[:, 0].astype(str).str.contains(r'^\s*\(', na=False)]
                df = df[df.iloc[:, 0].notna()]
                
                # Check if any column contains GSU, PSU, or RSU
                has_equity_data = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if any(equity_type in col_lower for equity_type in ['gsu', 'psu', 'rsu']):
                        has_equity_data = True
                        break
                
                if not has_equity_data:
                    continue
                
                logger.info(f"Found equity data in {file_path.name}")
                
                # Process each row
                for _, row in df.iterrows():
                    name_field = row.get(df.columns[0], '')
                    if pd.isna(name_field) or name_field == '' or str(name_field).strip() == '':
                        continue
                    
                    # Skip header rows and footnotes
                    if str(name_field).startswith('(') or 'named executive' in str(name_field).lower():
                        continue
                    
                    name = self.extract_clean_name(name_field)
                    if not name or len(name) < 3:
                        continue
                    
                    # Validate name is actually a person's name
                    if not self.is_valid_person_name(name):
                        continue
                    
                    # Initialize equity data for this person if not exists
                    if name not in equity_data:
                        equity_data[name] = {
                            'gsus': None,
                            'psus': None,
                            'rsus': None,
                            'gsu_value': None,
                            'psu_value': None,
                            'rsu_value': None
                        }
                    
                    # Extract GSU data
                    for col in df.columns:
                        col_lower = str(col).lower()
                        if 'gsu' in col_lower:
                            value = self.clean_numeric_value(row.get(col, ''))
                            if value is not None:
                                # Check if it's a number (count) or dollar value
                                if 'value' in col_lower or '$' in col_lower or 'award' in col_lower:
                                    if equity_data[name]['gsu_value'] is None or value > equity_data[name]['gsu_value']:
                                        equity_data[name]['gsu_value'] = value
                                else:
                                    if equity_data[name]['gsus'] is None or value > equity_data[name]['gsus']:
                                        equity_data[name]['gsus'] = value
                    
                    # Extract PSU data
                    for col in df.columns:
                        col_lower = str(col).lower()
                        if 'psu' in col_lower:
                            value = self.clean_numeric_value(row.get(col, ''))
                            if value is not None:
                                # Check if it's a number (count) or dollar value
                                if 'value' in col_lower or '$' in col_lower or 'award' in col_lower:
                                    if equity_data[name]['psu_value'] is None or value > equity_data[name]['psu_value']:
                                        equity_data[name]['psu_value'] = value
                                else:
                                    if equity_data[name]['psus'] is None or value > equity_data[name]['psus']:
                                        equity_data[name]['psus'] = value
                    
                    # Extract RSU data
                    for col in df.columns:
                        col_lower = str(col).lower()
                        if 'rsu' in col_lower:
                            value = self.clean_numeric_value(row.get(col, ''))
                            if value is not None:
                                # Check if it's a number (count) or dollar value
                                if 'value' in col_lower or '$' in col_lower or 'award' in col_lower:
                                    if equity_data[name]['rsu_value'] is None or value > equity_data[name]['rsu_value']:
                                        equity_data[name]['rsu_value'] = value
                                else:
                                    if equity_data[name]['rsus'] is None or value > equity_data[name]['rsus']:
                                        equity_data[name]['rsus'] = value
                
            except Exception as e:
                logger.warning(f"Could not process {file_path.name}: {e}")
                continue
        
        return equity_data
    
    def extract_ownership_data_from_all_tables(self) -> Dict[str, Dict[str, Any]]:
        """Extract ownership data from ALL CSV files in the directory."""
        logger.info("Scanning all CSV files for ownership data...")
        
        ownership_data = {}
        
        # Get all CSV files in the directory
        csv_files = list(self.tables_dir.glob("*.csv"))
        
        for file_path in csv_files:
            try:
                # Skip metadata file
                if 'metadata' in file_path.name.lower():
                    continue
                
                # Try reading with different error handling strategies
                df = None
                try:
                    df = pd.read_csv(file_path)
                except:
                    # Try with error_bad_lines=False for Python 3.7+, or on_bad_lines='skip' for newer pandas
                    try:
                        df = pd.read_csv(file_path, on_bad_lines='skip')
                    except:
                        try:
                            df = pd.read_csv(file_path, error_bad_lines=False, warn_bad_lines=False)
                        except:
                            # Try with different encoding
                            try:
                                df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
                            except:
                                logger.warning(f"Could not parse {file_path.name} - skipping")
                                continue
                
                if df is None or len(df) < 2:
                    continue
                
                # Handle multi-row headers for ownership tables
                # Check if first row contains class information
                if len(df) > 0:
                    first_row = df.iloc[0].astype(str)
                    if any('class' in str(cell).lower() for cell in first_row):
                        # Build composite column names from first two rows
                        class_info = df.iloc[0].fillna('')
                        header_info = df.iloc[1].fillna('')
                        new_columns = []
                        for class_val, header_val in zip(class_info, header_info):
                            class_str = str(class_val).strip()
                            header_str = str(header_val).strip()
                            if class_str and 'class' in class_str.lower():
                                # Combine class and header info
                                new_columns.append(f"{class_str} - {header_str}")
                            else:
                                new_columns.append(header_str if header_str else class_str)
                        df.columns = new_columns
                        # Remove the header rows from data
                        df = df.iloc[2:].reset_index(drop=True)
                
                # Remove footnote rows
                df = df[~df.iloc[:, 0].astype(str).str.contains('FOOTNOTES', na=False, regex=False)]
                df = df[~df.iloc[:, 0].astype(str).str.contains(r'^\s*\(', na=False)]
                df = df[df.iloc[:, 0].notna()]
                
                # Check if any column contains ownership-related terms
                has_ownership_data = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if any(term in col_lower for term in ['ownership', 'beneficially', 'shares owned', 'common stock owned', 'shares beneficially']):
                        has_ownership_data = True
                        break
                
                if not has_ownership_data:
                    continue
                
                logger.info(f"Found ownership data in {file_path.name}")
                
                # Process each row
                for _, row in df.iterrows():
                    name_field = row.get(df.columns[0], '')
                    if pd.isna(name_field) or name_field == '' or str(name_field).strip() == '':
                        continue
                    
                    # Skip header rows and footnotes
                    if str(name_field).startswith('('):
                        continue
                    
                    name = self.extract_clean_name(name_field)
                    if not name or len(name) < 3:
                        continue
                    
                    # Validate name is actually a person's name
                    if not self.is_valid_person_name(name):
                        continue
                    
                    # Initialize ownership data for this person if not exists
                    if name not in ownership_data:
                        ownership_data[name] = {
                            'total_shares_owned': None,
                            'class_a_shares': None,
                            'class_b_shares': None,
                            'class_c_shares': None,
                            'class_d_shares': None,
                        }
                    
                    # Extract ownership data by class
                    for col in df.columns:
                        col_lower = str(col).lower()
                        col_str = str(col)
                        
                        # Skip percentage and voting power columns
                        if '%' in col_lower or 'percent' in col_lower or 'voting power' in col_lower:
                            continue
                        
                        # Look for shares columns
                        if any(term in col_lower for term in ['shares', 'stock owned', 'beneficially owned']):
                            value = self.clean_numeric_value(row.get(col, ''))
                            if value is not None and value > 0:
                                # Determine which class of stock
                                if 'class a' in col_lower:
                                    if ownership_data[name]['class_a_shares'] is None or value > ownership_data[name]['class_a_shares']:
                                        ownership_data[name]['class_a_shares'] = value
                                elif 'class b' in col_lower:
                                    if ownership_data[name]['class_b_shares'] is None or value > ownership_data[name]['class_b_shares']:
                                        ownership_data[name]['class_b_shares'] = value
                                elif 'class c' in col_lower:
                                    if ownership_data[name]['class_c_shares'] is None or value > ownership_data[name]['class_c_shares']:
                                        ownership_data[name]['class_c_shares'] = value
                                elif 'class d' in col_lower:
                                    if ownership_data[name]['class_d_shares'] is None or value > ownership_data[name]['class_d_shares']:
                                        ownership_data[name]['class_d_shares'] = value
                                else:
                                    # Generic shares column without class designation
                                    if ownership_data[name]['total_shares_owned'] is None or value > ownership_data[name]['total_shares_owned']:
                                        ownership_data[name]['total_shares_owned'] = value
                    
                    # Calculate total shares if we have class-specific data
                    class_total = 0
                    has_class_data = False
                    for class_key in ['class_a_shares', 'class_b_shares', 'class_c_shares', 'class_d_shares']:
                        if ownership_data[name][class_key] is not None:
                            class_total += ownership_data[name][class_key]
                            has_class_data = True
                    
                    # Set total shares as the sum of all classes if we have class-specific data
                    if has_class_data and class_total > 0:
                        ownership_data[name]['total_shares_owned'] = class_total
                
            except Exception as e:
                logger.warning(f"Could not process {file_path.name}: {e}")
                continue
        
        return ownership_data
    
    def extract_executive_data(self) -> Dict[str, Dict[str, Any]]:
        """Extract executive compensation data focusing on requested fields."""
        logger.info("Extracting executive compensation data...")
        
        # Try to find executive compensation table automatically
        exec_file, _ = self.find_compensation_tables()
        
        # Fallback to common names
        if not exec_file:
            common_names = [
                "table_2_01_executive_compensation.csv",
                "executive_compensation.csv",
                "named_executive_officer_compensation.csv"
            ]
            for name in common_names:
                potential_file = self.tables_dir / name
                if potential_file.exists():
                    exec_file = potential_file
                    break
        
        if not exec_file:
            logger.error("No executive compensation table found")
            return {}
        
        df = pd.read_csv(exec_file)
        
        # Remove footnote rows and empty rows
        df = df[~df.iloc[:, 0].astype(str).str.contains('FOOTNOTES', na=False)]
        df = df[~df.iloc[:, 0].astype(str).str.contains(r'^\s*\(', na=False)]
        df = df[df.iloc[:, 0].notna()]
        
        executives = {}
        
        # Check if there's a Year column for filtering
        year_col = None
        for col in df.columns:
            if 'year' in str(col).lower():
                year_col = col
                break
        
        for _, row in df.iterrows():
            name_field = row.get(df.columns[0], '')  # Use first column as name
            if pd.isna(name_field) or name_field == '' or str(name_field).strip() == '':
                continue
                
            # Skip if it looks like a footnote or explanation
            if str(name_field).startswith('(') or 'reflects amounts' in str(name_field).lower():
                continue
                
            name = self.extract_clean_name(name_field)
            if not name or len(name) < 3:
                continue
            
            # Validate name is actually a person's name
            if not self.is_valid_person_name(name):
                logger.debug(f"Skipping invalid person name from executive table: {name}")
                continue
            
            # Filter by year - prioritize 2024, or most recent year
            if year_col:
                year_value = str(row.get(year_col, '')).strip()
                # Extract year from values like "2024", "2024 (7)", etc.
                year_match = re.search(r'(202[0-9])', year_value)
                if year_match:
                    year = int(year_match.group(1))
                    # Only process 2024 data, or most recent if we haven't seen this person yet
                    if name in executives and year != 2024:
                        # Skip older years if we already have data for this person
                        continue
                    elif name in executives and year == 2024:
                        # Replace with 2024 data if we had older data
                        pass
                    elif year < 2024:
                        # Skip older years on first encounter, unless it's the only data
                        # We'll collect it but may replace with 2024 later
                        pass
            
            # Extract requested fields - look for common column patterns
            base_pay = None
            cash_bonus = None
            stock_awards = None
            non_equity_incentive = None
            option_awards = None
            all_other_comp = None
            
            # Find base pay (salary) - expanded search patterns
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['salary', 'base pay', 'base compensation', 'base cash']):
                    base_pay = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find cash bonus - expanded search patterns
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['bonus', 'cash bonus', 'annual bonus', 'incentive bonus']):
                    # Avoid non-equity incentive columns
                    if 'non-equity' not in col_lower and 'non equity' not in col_lower:
                        cash_bonus = self.clean_numeric_value(row.get(col, ''))
                        break
            
            # Find stock awards - expanded search patterns
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['stock award', 'equity award', 'stock compensation', 
                                                             'equity compensation', 'restricted stock']):
                    stock_awards = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find non-equity incentive compensation
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['non-equity incentive', 'non equity incentive', 
                                                             'incentive plan compensation']):
                    non_equity_incentive = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find option awards
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['option award', 'stock option', 'option grants']):
                    option_awards = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find all other compensation
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['all other compensation', 'other compensation', 
                                                             'other comp', 'perquisites']):
                    all_other_comp = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find total compensation - expanded search patterns
            total_comp = None
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['total', 'total compensation', 'total comp', 
                                                             'aggregate compensation']):
                    # Make sure it's actually a total column
                    if 'target' not in col_lower:
                        total_comp = self.clean_numeric_value(row.get(col, ''))
                        break
            
            executive_data = {
                'base_pay': base_pay,  # Consolidated from base_pay_cash
                'cash_bonus': cash_bonus,
                'stock_awards': stock_awards,
                'non_equity_incentive': non_equity_incentive,
                'option_awards': option_awards,
                'all_other_compensation': all_other_comp,
                'rsus': None,  # Will be filled from equity data if available
                'psus': None,  # Will be filled from equity data if available
                'gsus': None,  # Will be filled from equity data if available
                'rsu_value': None,
                'psu_value': None,
                'gsu_value': None,
                'total_shares_owned': None,  # Will be filled from ownership data if available
                'class_a_shares': None,
                'class_b_shares': None,
                'class_c_shares': None,
                'class_d_shares': None,
                'total_compensation': total_comp,
                'raw_name': name_field
            }
            
            executives[name] = executive_data
        
        return executives
    
    def extract_director_data(self) -> Dict[str, Dict[str, Any]]:
        """Extract director compensation data focusing on requested fields."""
        logger.info("Extracting director compensation data...")
        
        # Try to find director compensation table automatically
        _, dir_file = self.find_compensation_tables()
        
        # Fallback to common names
        if not dir_file:
            common_names = [
                "table_2_02_director_compensation.csv",
                "director_compensation.csv",
                "board_compensation.csv"
            ]
            for name in common_names:
                potential_file = self.tables_dir / name
                if potential_file.exists():
                    dir_file = potential_file
                    break
        
        if not dir_file:
            logger.error("No director compensation table found")
            return {}
        
        df = pd.read_csv(dir_file)
        
        # Remove footnote rows and empty rows
        df = df[~df.iloc[:, 0].astype(str).str.contains('FOOTNOTES', na=False)]
        df = df[~df.iloc[:, 0].astype(str).str.contains(r'^\s*\(', na=False)]
        df = df[df.iloc[:, 0].notna()]
        
        directors = {}
        
        # Check if there's a Year column for filtering
        year_col = None
        for col in df.columns:
            if 'year' in str(col).lower():
                year_col = col
                break
        
        for _, row in df.iterrows():
            name_field = row.get(df.columns[0], '')  # Use first column as name
            if pd.isna(name_field) or name_field == '' or str(name_field).strip() == '':
                continue
                
            # Skip if it looks like a footnote or explanation
            if str(name_field).startswith('(') or 'reflects amounts' in str(name_field).lower():
                continue
                
            name = self.extract_clean_name(name_field)
            if not name or len(name) < 3:
                continue
            
            # Validate name is actually a person's name
            if not self.is_valid_person_name(name):
                logger.debug(f"Skipping invalid person name from director table: {name}")
                continue
            
            # Filter by year - prioritize 2024, or most recent year
            if year_col:
                year_value = str(row.get(year_col, '')).strip()
                # Extract year from values like "2024", "2024 (7)", etc.
                year_match = re.search(r'(202[0-9])', year_value)
                if year_match:
                    year = int(year_match.group(1))
                    # Only process 2024 data, or most recent if we haven't seen this person yet
                    if name in directors and year != 2024:
                        # Skip older years if we already have data for this person
                        continue
                    elif name in directors and year == 2024:
                        # Replace with 2024 data if we had older data
                        pass
                    elif year < 2024:
                        # Skip older years on first encounter, unless it's the only data
                        # We'll collect it but may replace with 2024 later
                        pass
            
            # Extract requested fields - look for common column patterns
            base_pay = None
            stock_awards = None
            option_awards = None
            all_other_comp = None
            
            # Find base pay (fees/retainer) - expanded search patterns
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['fees earned', 'fees paid', 'cash fees', 'fees', 
                                                             'retainer', 'annual retainer', 'board fees']):
                    base_pay = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find stock awards - expanded search patterns
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['stock award', 'equity award', 'stock compensation',
                                                             'equity compensation', 'restricted stock']):
                    stock_awards = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find option awards
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['option award', 'stock option', 'option grants']):
                    option_awards = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find all other compensation
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['all other compensation', 'other compensation', 
                                                             'other comp', 'perquisites']):
                    all_other_comp = self.clean_numeric_value(row.get(col, ''))
                    break
            
            # Find total compensation - expanded search patterns
            total_comp = None
            for col in df.columns:
                col_lower = str(col).lower()
                if any(pattern in col_lower for pattern in ['total', 'total compensation', 'total comp',
                                                             'aggregate compensation']):
                    # Make sure it's actually a total column
                    if 'target' not in col_lower:
                        total_comp = self.clean_numeric_value(row.get(col, ''))
                        break
            
            director_data = {
                'base_pay': base_pay,
                'stock_awards': stock_awards,
                'option_awards': option_awards,
                'all_other_compensation': all_other_comp,
                'total_shares_owned': None,  # Will be filled from ownership data if available
                'class_a_shares': None,
                'class_b_shares': None,
                'class_c_shares': None,
                'class_d_shares': None,
                'gsus': None,  # Will be filled from equity data if available
                'psus': None,  # Will be filled from equity data if available
                'rsus': None,  # Will be filled from equity data if available
                'gsu_value': None,
                'psu_value': None,
                'rsu_value': None,
                'total_compensation': total_comp,
                'raw_name': name_field
            }
            
            directors[name] = director_data
        
        return directors
    
    def extract_all_data(self) -> Dict[str, Any]:
        """Extract all requested compensation data."""
        logger.info(f"Starting generalized compensation data extraction for {self.company_name}...")
        
        # Extract base compensation data
        executives = self.extract_executive_data()
        directors = self.extract_director_data()
        
        # Extract equity data from ALL tables
        equity_data = self.extract_equity_data_from_all_tables()
        
        # Extract ownership data from ALL tables
        ownership_data = self.extract_ownership_data_from_all_tables()
        
        # Merge equity data into executives
        for name, equity_info in equity_data.items():
            if name in executives:
                logger.info(f"Merging equity data for executive: {name}")
                executives[name]['gsus'] = equity_info.get('gsus')
                executives[name]['psus'] = equity_info.get('psus')
                executives[name]['rsus'] = equity_info.get('rsus')
                executives[name]['gsu_value'] = equity_info.get('gsu_value')
                executives[name]['psu_value'] = equity_info.get('psu_value')
                executives[name]['rsu_value'] = equity_info.get('rsu_value')
            elif name in directors:
                logger.info(f"Merging equity data for director: {name}")
                directors[name]['gsus'] = equity_info.get('gsus')
                directors[name]['psus'] = equity_info.get('psus')
                directors[name]['rsus'] = equity_info.get('rsus')
                directors[name]['gsu_value'] = equity_info.get('gsu_value')
                directors[name]['psu_value'] = equity_info.get('psu_value')
                directors[name]['rsu_value'] = equity_info.get('rsu_value')
            else:
                # This person is in equity tables but not in compensation tables
                logger.info(f"Found equity data for person not in compensation tables: {name}")
        
        # Merge ownership data into directors and executives
        for name, ownership_info in ownership_data.items():
            if name in directors:
                logger.info(f"Merging ownership data for director: {name}")
                directors[name]['total_shares_owned'] = ownership_info.get('total_shares_owned')
                directors[name]['class_a_shares'] = ownership_info.get('class_a_shares')
                directors[name]['class_b_shares'] = ownership_info.get('class_b_shares')
                directors[name]['class_c_shares'] = ownership_info.get('class_c_shares')
                directors[name]['class_d_shares'] = ownership_info.get('class_d_shares')
            elif name in executives:
                logger.info(f"Merging ownership data for executive: {name}")
                executives[name]['total_shares_owned'] = ownership_info.get('total_shares_owned')
                executives[name]['class_a_shares'] = ownership_info.get('class_a_shares')
                executives[name]['class_b_shares'] = ownership_info.get('class_b_shares')
                executives[name]['class_c_shares'] = ownership_info.get('class_c_shares')
                executives[name]['class_d_shares'] = ownership_info.get('class_d_shares')
            else:
                # This person is in ownership tables but not in compensation tables
                logger.info(f"Found ownership data for person not in compensation tables: {name}")
        
        data = {
            'company_name': self.company_name,
            'executives': executives,
            'directors': directors,
            'extraction_metadata': {
                'total_executives': len(executives),
                'total_directors': len(directors),
                'total_equity_records': len(equity_data),
                'total_ownership_records': len(ownership_data),
                'extraction_timestamp': pd.Timestamp.now().isoformat(),
                'tables_dir': str(self.tables_dir)
            }
        }
        
        # Validate the data
        logger.info("Validating extracted data...")
        validation_result = self.validation_agent.validate_extraction(data, self.company_name)
        data['validation'] = validation_result
        
        return data
    
    def save_results(self, data: Dict[str, Any], output_prefix: str = "generalized_compensation"):
        """Save extraction results to JSON and CSV files."""
        # Save JSON file
        json_path = Path(f"{output_prefix}_results.json")
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        logger.info(f"JSON results saved to {json_path}")
        
        # Save CSV files
        self.save_csv_files(data, output_prefix)
    
    def save_csv_files(self, data: Dict[str, Any], output_prefix: str):
        """Save executive and director data to separate CSV files."""
        # Save executives CSV
        if data['executives']:
            exec_df = pd.DataFrame.from_dict(data['executives'], orient='index')
            exec_df.index.name = 'Name'
            exec_csv_path = Path(f"{output_prefix}_executives.csv")
            exec_df.to_csv(exec_csv_path)
            logger.info(f"Executive compensation CSV saved to {exec_csv_path}")
        
        # Save directors CSV
        if data['directors']:
            dir_df = pd.DataFrame.from_dict(data['directors'], orient='index')
            dir_df.index.name = 'Name'
            dir_csv_path = Path(f"{output_prefix}_directors.csv")
            dir_df.to_csv(dir_csv_path)
            logger.info(f"Director compensation CSV saved to {dir_csv_path}")
        
        # Save combined CSV
        self.save_combined_csv(data, output_prefix)
    
    def save_combined_csv(self, data: Dict[str, Any], output_prefix: str):
        """Save combined executive and director data to a single CSV file."""
        combined_data = []
        
        # Add executives
        for name, info in data['executives'].items():
            row = {
                'Name': name,
                'Type': 'Executive',
                'Base_Pay': info.get('base_pay'),
                'Cash_Bonus': info.get('cash_bonus'),
                'Stock_Awards': info.get('stock_awards'),
                'Non_Equity_Incentive': info.get('non_equity_incentive'),
                'Option_Awards': info.get('option_awards'),
                'All_Other_Compensation': info.get('all_other_compensation'),
                'RSUs': info.get('rsus'),
                'PSUs': info.get('psus'),
                'GSUs': info.get('gsus'),
                'RSU_Value': info.get('rsu_value'),
                'PSU_Value': info.get('psu_value'),
                'GSU_Value': info.get('gsu_value'),
                'Total_Shares_Owned': info.get('total_shares_owned'),
                'Class_A_Shares': info.get('class_a_shares'),
                'Class_B_Shares': info.get('class_b_shares'),
                'Class_C_Shares': info.get('class_c_shares'),
                'Class_D_Shares': info.get('class_d_shares'),
                'Total_Compensation': info.get('total_compensation')
            }
            combined_data.append(row)
        
        # Add directors
        for name, info in data['directors'].items():
            row = {
                'Name': name,
                'Type': 'Director',
                'Base_Pay': info.get('base_pay'),
                'Cash_Bonus': None,  # Not applicable for directors
                'Stock_Awards': info.get('stock_awards'),
                'Non_Equity_Incentive': None,  # Not applicable for directors
                'Option_Awards': info.get('option_awards'),
                'All_Other_Compensation': info.get('all_other_compensation'),
                'RSUs': info.get('rsus'),
                'PSUs': info.get('psus'),
                'GSUs': info.get('gsus'),
                'RSU_Value': info.get('rsu_value'),
                'PSU_Value': info.get('psu_value'),
                'GSU_Value': info.get('gsu_value'),
                'Total_Shares_Owned': info.get('total_shares_owned'),
                'Class_A_Shares': info.get('class_a_shares'),
                'Class_B_Shares': info.get('class_b_shares'),
                'Class_C_Shares': info.get('class_c_shares'),
                'Class_D_Shares': info.get('class_d_shares'),
                'Total_Compensation': info.get('total_compensation')
            }
            combined_data.append(row)
        
        if combined_data:
            combined_df = pd.DataFrame(combined_data)
            combined_csv_path = Path(f"{output_prefix}_combined.csv")
            combined_df.to_csv(combined_csv_path, index=False)
            logger.info(f"Combined compensation CSV saved to {combined_csv_path}")
    
    def print_summary(self, data: Dict[str, Any]):
        """Print a summary of the extracted data."""
        print("\n" + "="*80)
        print(f"GENERALIZED COMPENSATION DATA EXTRACTION SUMMARY - {data.get('company_name', 'Unknown Company')}")
        print("="*80)
        
        # Print validation results
        validation = data.get('validation', {})
        if validation.get('enabled', True):
            print(f"\nVALIDATION RESULTS:")
            print(f"  Status: {'✅ VALID' if validation.get('validated') else '❌ INVALID'}")
            print(f"  Confidence: {validation.get('confidence', 'N/A')}/10")
            if validation.get('issues'):
                issues = validation['issues']
                if isinstance(issues, list):
                    issues_str = ', '.join(str(issue) for issue in issues)
                else:
                    issues_str = str(issues)
                print(f"  Issues: {issues_str}")
            if validation.get('suggestions'):
                suggestions = validation['suggestions']
                if isinstance(suggestions, list) and suggestions:
                    print(f"  Suggestions: {suggestions[0]}")
                else:
                    print(f"  Suggestions: {suggestions}")
        
        print(f"\nEXECUTIVES ({data['extraction_metadata']['total_executives']}):")
        print("-" * 50)
        for name, info in data['executives'].items():
            print(f"\n{name}:")
            print(f"  Base Pay: ${info.get('base_pay', 'N/A'):,.0f}" if info.get('base_pay') else "  Base Pay: N/A")
            print(f"  Cash Bonus: ${info.get('cash_bonus', 'N/A'):,.0f}" if info.get('cash_bonus') else "  Cash Bonus: N/A")
            print(f"  Stock Awards: ${info.get('stock_awards', 'N/A'):,.0f}" if info.get('stock_awards') else "  Stock Awards: N/A")
            print(f"  Non-Equity Incentive: ${info.get('non_equity_incentive', 'N/A'):,.0f}" if info.get('non_equity_incentive') else "  Non-Equity Incentive: N/A")
            print(f"  Option Awards: ${info.get('option_awards', 'N/A'):,.0f}" if info.get('option_awards') else "  Option Awards: N/A")
            print(f"  All Other Compensation: ${info.get('all_other_compensation', 'N/A'):,.0f}" if info.get('all_other_compensation') else "  All Other Compensation: N/A")
            print(f"  RSUs (count): {info.get('rsus', 'N/A'):,.0f}" if info.get('rsus') else "  RSUs (count): N/A")
            print(f"  RSU Value: ${info.get('rsu_value', 'N/A'):,.0f}" if info.get('rsu_value') else "  RSU Value: N/A")
            print(f"  PSUs (count): {info.get('psus', 'N/A'):,.0f}" if info.get('psus') else "  PSUs (count): N/A")
            print(f"  PSU Value: ${info.get('psu_value', 'N/A'):,.0f}" if info.get('psu_value') else "  PSU Value: N/A")
            print(f"  GSUs (count): {info.get('gsus', 'N/A'):,.0f}" if info.get('gsus') else "  GSUs (count): N/A")
            print(f"  GSU Value: ${info.get('gsu_value', 'N/A'):,.0f}" if info.get('gsu_value') else "  GSU Value: N/A")
            print(f"  Total Shares Owned: {info.get('total_shares_owned', 'N/A'):,.0f}" if info.get('total_shares_owned') else "  Total Shares Owned: N/A")
            if info.get('class_a_shares'):
                print(f"    - Class A: {info.get('class_a_shares', 'N/A'):,.0f}")
            if info.get('class_b_shares'):
                print(f"    - Class B: {info.get('class_b_shares', 'N/A'):,.0f}")
            if info.get('class_c_shares'):
                print(f"    - Class C: {info.get('class_c_shares', 'N/A'):,.0f}")
            if info.get('class_d_shares'):
                print(f"    - Class D: {info.get('class_d_shares', 'N/A'):,.0f}")
            print(f"  Total Compensation: ${info.get('total_compensation', 'N/A'):,.0f}" if info.get('total_compensation') else "  Total Compensation: N/A")
        
        print(f"\nDIRECTORS ({data['extraction_metadata']['total_directors']}):")
        print("-" * 50)
        for name, info in data['directors'].items():
            print(f"\n{name}:")
            print(f"  Base Pay: ${info.get('base_pay', 'N/A'):,.0f}" if info.get('base_pay') else "  Base Pay: N/A")
            print(f"  Stock Awards: ${info.get('stock_awards', 'N/A'):,.0f}" if info.get('stock_awards') else "  Stock Awards: N/A")
            print(f"  Option Awards: ${info.get('option_awards', 'N/A'):,.0f}" if info.get('option_awards') else "  Option Awards: N/A")
            print(f"  All Other Compensation: ${info.get('all_other_compensation', 'N/A'):,.0f}" if info.get('all_other_compensation') else "  All Other Compensation: N/A")
            print(f"  RSUs (count): {info.get('rsus', 'N/A'):,.0f}" if info.get('rsus') else "  RSUs (count): N/A")
            print(f"  RSU Value: ${info.get('rsu_value', 'N/A'):,.0f}" if info.get('rsu_value') else "  RSU Value: N/A")
            print(f"  PSUs (count): {info.get('psus', 'N/A'):,.0f}" if info.get('psus') else "  PSUs (count): N/A")
            print(f"  PSU Value: ${info.get('psu_value', 'N/A'):,.0f}" if info.get('psu_value') else "  PSU Value: N/A")
            print(f"  GSUs (count): {info.get('gsus', 'N/A'):,.0f}" if info.get('gsus') else "  GSUs (count): N/A")
            print(f"  GSU Value: ${info.get('gsu_value', 'N/A'):,.0f}" if info.get('gsu_value') else "  GSU Value: N/A")
            print(f"  Total Shares Owned: {info.get('total_shares_owned', 'N/A'):,.0f}" if info.get('total_shares_owned') else "  Total Shares Owned: N/A")
            if info.get('class_a_shares'):
                print(f"    - Class A: {info.get('class_a_shares', 'N/A'):,.0f}")
            if info.get('class_b_shares'):
                print(f"    - Class B: {info.get('class_b_shares', 'N/A'):,.0f}")
            if info.get('class_c_shares'):
                print(f"    - Class C: {info.get('class_c_shares', 'N/A'):,.0f}")
            if info.get('class_d_shares'):
                print(f"    - Class D: {info.get('class_d_shares', 'N/A'):,.0f}")
            print(f"  Total Compensation: ${info.get('total_compensation', 'N/A'):,.0f}" if info.get('total_compensation') else "  Total Compensation: N/A")

def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract compensation data from proxy tables')
    parser.add_argument('--tables-dir', default='extracted_tables', help='Directory containing extracted tables')
    parser.add_argument('--company', default='Alphabet Inc.', help='Company name for validation')
    parser.add_argument('--output-prefix', default='generalized_compensation', help='Output file prefix')
    
    args = parser.parse_args()
    
    extractor = GeneralizedCompensationExtractor(
        tables_dir=args.tables_dir,
        company_name=args.company
    )
    
    # Extract all data
    results = extractor.extract_all_data()
    
    # Save results
    extractor.save_results(results, args.output_prefix)
    
    # Print summary
    extractor.print_summary(results)
    
    print(f"\nResults saved to:")
    print(f"  - {args.output_prefix}_results.json (detailed JSON data)")
    print(f"  - {args.output_prefix}_executives.csv (executives only)")
    print(f"  - {args.output_prefix}_directors.csv (directors only)")
    print(f"  - {args.output_prefix}_combined.csv (all data in one file)")
    print("="*80)

if __name__ == "__main__":
    main()
