#!/usr/bin/env python3
"""
Focused Compensation Data Extractor

This script extracts specific compensation fields for executives and directors
as requested by the user:

Executives:
- Base Pay (cash)
- Cash bonus  
- Stock Options granted (if available)
- RSUs (GSUs)
- PSUs
- GSUs

Directors:
- Base pay
- Shares granted
- Total shares owned
"""

import pandas as pd
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FocusedCompensationExtractor:
    def __init__(self, tables_dir: str = "extracted_tables"):
        self.tables_dir = Path(tables_dir)
        
    def clean_numeric_value(self, value: str) -> Optional[float]:
        """Clean and convert numeric values from compensation tables."""
        if pd.isna(value) or value == '' or value == '-':
            return None
            
        # Remove commas, parentheses, and other formatting
        cleaned = str(value).replace(',', '').replace('$', '').replace('(', '').replace(')', '')
        
        # Handle footnote references like "405,630 (5)"
        cleaned = re.sub(r'\s*\(\d+\)', '', cleaned)
        
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    def extract_name(self, name_field: str) -> str:
        """Extract clean name from name field."""
        if pd.isna(name_field):
            return ""
        
        # Remove position titles and clean up the name
        name = str(name_field)
        
        # Common patterns to remove
        patterns_to_remove = [
            r'\s+and\s+.*',  # Remove "and Google, and Director" etc.
            r'\s+Senior\s+Vice\s+President.*',
            r'\s+President\s+and\s+.*',
            r'\s+Chief\s+.*',
            r'\s+Legal\s+Officer.*',
            r'\s+Secretary.*',
            r'\s+until\s+.*',
            r'\s+as\s+of\s+.*',
            r'\s+through\s+.*',
            r'\s+\(.*\)',  # Remove parenthetical content
            r'\s+;.*',  # Remove everything after semicolon
        ]
        
        for pattern in patterns_to_remove:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        # Additional cleanup for specific cases
        name = re.sub(r'\s+,\s+.*', '', name)  # Remove everything after comma
        name = re.sub(r'\s+until.*', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s+as\s+of.*', '', name, flags=re.IGNORECASE)
        
        return name.strip()
    
    def extract_executive_data(self) -> Dict[str, Dict[str, Any]]:
        """Extract executive compensation data focusing on requested fields."""
        logger.info("Extracting executive compensation data...")
        
        # Load main executive compensation table
        exec_file = self.tables_dir / "table_2_01_executive_compensation.csv"
        if not exec_file.exists():
            logger.error(f"Executive compensation file not found: {exec_file}")
            return {}
        
        df = pd.read_csv(exec_file)
        
        # Remove footnote rows and empty rows
        df = df[~df.iloc[:, 0].astype(str).str.contains('FOOTNOTES', na=False)]
        df = df[~df.iloc[:, 0].astype(str).str.contains(r'^\s*\(', na=False)]
        df = df[df.iloc[:, 0].notna()]
        
        # Load equity awards data
        equity_file = self.tables_dir / "table_2_03_equity_awards.csv"
        equity_data = {}
        if equity_file.exists():
            equity_df = pd.read_csv(equity_file)
            equity_df = equity_df[~equity_df.iloc[:, 0].astype(str).str.contains('FOOTNOTES', na=False)]
            
            for _, row in equity_df.iterrows():
                name_field = row.get('Named Executive', '')
                if pd.isna(name_field) or name_field == '':
                    continue
                name = self.extract_name(name_field)
                if name and len(name) > 3:
                    equity_data[name] = {
                        'gsus_granted': self.clean_numeric_value(row.get('Number of GSUs Granted (1)', '')),
                        'psus_granted': self.clean_numeric_value(row.get('Number of PSUs Granted (1)', '')),
                        'gsu_award_value': self.clean_numeric_value(row.get('Target GSU Award Value ($)', '')),
                        'psu_award_value': self.clean_numeric_value(row.get('Target PSU Award Value ($)', ''))
                    }
        
        executives = {}
        
        for _, row in df.iterrows():
            name_field = row.get('Name and Principal Position', '')
            if pd.isna(name_field) or name_field == '' or str(name_field).strip() == '':
                continue
                
            # Skip if it looks like a footnote or explanation
            if str(name_field).startswith('(') or 'reflects amounts' in str(name_field).lower():
                continue
                
            name = self.extract_name(name_field)
            if not name or len(name) < 3:
                continue
            
            # Extract requested fields
            base_pay = self.clean_numeric_value(row.get('Salary ($) (1)', ''))
            cash_bonus = self.clean_numeric_value(row.get('Bonus ($)', ''))
            non_equity_incentive = self.clean_numeric_value(row.get('Non-Equity Incentive Plan Compensation  ($) (3)', ''))
            stock_awards = self.clean_numeric_value(row.get('Stock Awards ($) (2)', ''))
            
            # Combine cash bonus and non-equity incentive as total cash bonus
            total_cash_bonus = (cash_bonus or 0) + (non_equity_incentive or 0)
            
            executive_data = {
                'base_pay_cash': base_pay,
                'cash_bonus': total_cash_bonus if total_cash_bonus > 0 else None,
                'stock_options_granted': None,  # Alphabet doesn't use traditional stock options
                'rsus': None,  # Will be filled from equity data
                'psus': None,  # Will be filled from equity data
                'gsus': None,  # Will be filled from equity data
                'total_compensation': self.clean_numeric_value(row.get('Total ($)', ''))
            }
            
            # Add equity data if available
            if name in equity_data:
                equity = equity_data[name]
                executive_data.update({
                    'rsus': equity['gsus_granted'],  # GSUs are similar to RSUs
                    'psus': equity['psus_granted'],
                    'gsus': equity['gsus_granted']
                })
            
            executives[name] = executive_data
        
        return executives
    
    def extract_director_data(self) -> Dict[str, Dict[str, Any]]:
        """Extract director compensation data focusing on requested fields."""
        logger.info("Extracting director compensation data...")
        
        # Load director compensation table
        director_file = self.tables_dir / "table_2_02_director_compensation.csv"
        if not director_file.exists():
            logger.error(f"Director compensation file not found: {director_file}")
            return {}
        
        df = pd.read_csv(director_file)
        
        # Remove footnote rows and empty rows
        df = df[~df.iloc[:, 0].astype(str).str.contains('FOOTNOTES', na=False)]
        df = df[~df.iloc[:, 0].astype(str).str.contains(r'^\s*\(', na=False)]
        df = df[df.iloc[:, 0].notna()]
        
        # Load ownership data
        ownership_file = self.tables_dir / "table_2_11_ownership.csv"
        ownership_data = {}
        if ownership_file.exists():
            ownership_df = pd.read_csv(ownership_file)
            
            for _, row in ownership_df.iterrows():
                name_field = row.get('Name of Beneficial Owner', '')
                if pd.isna(name_field) or name_field == '':
                    continue
                name = self.extract_name(name_field)
                if name and len(name) > 3:
                    class_a_shares = self.clean_numeric_value(row.get('Shares', ''))
                    class_b_shares = self.clean_numeric_value(row.get('Shares.1', ''))
                    total_shares = (class_a_shares or 0) + (class_b_shares or 0)
                    
                    ownership_data[name] = {
                        'total_shares_owned': total_shares if total_shares > 0 else None,
                        'class_a_shares': class_a_shares,
                        'class_b_shares': class_b_shares
                    }
        
        directors = {}
        
        for _, row in df.iterrows():
            name_field = row.get('Name', '')
            if pd.isna(name_field) or name_field == '' or str(name_field).strip() == '':
                continue
                
            # Skip if it looks like a footnote or explanation
            if str(name_field).startswith('(') or 'reflects amounts' in str(name_field).lower():
                continue
                
            name = self.extract_name(name_field)
            if not name or len(name) < 3:
                continue
            
            # Extract shares granted from footnotes (stored in the CSV)
            shares_granted = None
            raw_name = str(name_field)
            if 'Frances' in raw_name or 'John Doerr' in raw_name or 'Roger' in raw_name or 'Ram' in raw_name or 'Robin' in raw_name:
                shares_granted = 5040  # From footnote (2)
            elif 'Marty' in raw_name:
                shares_granted = 7000  # From footnote (4)
            elif 'John Hennessy' in raw_name:
                shares_granted = 7181  # From footnote (5)
            
            director_data = {
                'base_pay': self.clean_numeric_value(row.get('Fees Earned or Paid in Cash ($)', '')),
                'shares_granted': shares_granted,
                'total_shares_owned': None,  # Will be filled from ownership data
                'total_compensation': self.clean_numeric_value(row.get('Total ($)', ''))
            }
            
            # Add ownership data if available
            if name in ownership_data:
                ownership = ownership_data[name]
                director_data['total_shares_owned'] = ownership['total_shares_owned']
            
            directors[name] = director_data
        
        return directors
    
    def extract_all_data(self) -> Dict[str, Any]:
        """Extract all requested compensation data."""
        logger.info("Starting focused compensation data extraction...")
        
        executives = self.extract_executive_data()
        directors = self.extract_director_data()
        
        return {
            'executives': executives,
            'directors': directors,
            'extraction_metadata': {
                'total_executives': len(executives),
                'total_directors': len(directors),
                'extraction_timestamp': pd.Timestamp.now().isoformat()
            }
        }
    
    def save_results(self, data: Dict[str, Any], output_file: str = "focused_compensation_results.json"):
        """Save extraction results to JSON and CSV files."""
        output_path = Path(output_file)
        
        # Save JSON file
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"JSON results saved to {output_path}")
        
        # Save CSV files
        self.save_csv_files(data)
    
    def save_csv_files(self, data: Dict[str, Any]):
        """Save executive and director data to separate CSV files."""
        # Save executives CSV
        if data['executives']:
            exec_df = pd.DataFrame.from_dict(data['executives'], orient='index')
            exec_df.index.name = 'Name'
            exec_csv_path = Path("executive_compensation.csv")
            exec_df.to_csv(exec_csv_path)
            logger.info(f"Executive compensation CSV saved to {exec_csv_path}")
        
        # Save directors CSV
        if data['directors']:
            dir_df = pd.DataFrame.from_dict(data['directors'], orient='index')
            dir_df.index.name = 'Name'
            dir_csv_path = Path("director_compensation.csv")
            dir_df.to_csv(dir_csv_path)
            logger.info(f"Director compensation CSV saved to {dir_csv_path}")
        
        # Save combined CSV
        self.save_combined_csv(data)
    
    def save_combined_csv(self, data: Dict[str, Any]):
        """Save combined executive and director data to a single CSV file."""
        combined_data = []
        
        # Add executives
        for name, info in data['executives'].items():
            row = {
                'Name': name,
                'Type': 'Executive',
                'Base_Pay_Cash': info.get('base_pay_cash'),
                'Cash_Bonus': info.get('cash_bonus'),
                'Stock_Options_Granted': info.get('stock_options_granted'),
                'RSUs': info.get('rsus'),
                'PSUs': info.get('psus'),
                'GSUs': info.get('gsus'),
                'Base_Pay': None,  # Not applicable for executives
                'Shares_Granted': None,  # Not applicable for executives
                'Total_Shares_Owned': None,  # Not applicable for executives
                'Total_Compensation': info.get('total_compensation')
            }
            combined_data.append(row)
        
        # Add directors
        for name, info in data['directors'].items():
            row = {
                'Name': name,
                'Type': 'Director',
                'Base_Pay_Cash': None,  # Not applicable for directors
                'Cash_Bonus': None,  # Not applicable for directors
                'Stock_Options_Granted': None,  # Not applicable for directors
                'RSUs': None,  # Not applicable for directors
                'PSUs': None,  # Not applicable for directors
                'GSUs': None,  # Not applicable for directors
                'Base_Pay': info.get('base_pay'),
                'Shares_Granted': info.get('shares_granted'),
                'Total_Shares_Owned': info.get('total_shares_owned'),
                'Total_Compensation': info.get('total_compensation')
            }
            combined_data.append(row)
        
        if combined_data:
            combined_df = pd.DataFrame(combined_data)
            combined_csv_path = Path("combined_compensation.csv")
            combined_df.to_csv(combined_csv_path, index=False)
            logger.info(f"Combined compensation CSV saved to {combined_csv_path}")
    
    def print_summary(self, data: Dict[str, Any]):
        """Print a summary of the extracted data."""
        print("\n" + "="*80)
        print("FOCUSED COMPENSATION DATA EXTRACTION SUMMARY")
        print("="*80)
        
        print(f"\nEXECUTIVES ({data['extraction_metadata']['total_executives']}):")
        print("-" * 50)
        for name, info in data['executives'].items():
            print(f"\n{name}:")
            print(f"  Base Pay (Cash): ${info.get('base_pay_cash', 'N/A'):,.0f}" if info.get('base_pay_cash') else "  Base Pay (Cash): N/A")
            print(f"  Cash Bonus: ${info.get('cash_bonus', 'N/A'):,.0f}" if info.get('cash_bonus') else "  Cash Bonus: N/A")
            print(f"  Stock Options Granted: {info.get('stock_options_granted', 'N/A')}")
            print(f"  RSUs (GSUs): {info.get('rsus', 'N/A'):,.0f}" if info.get('rsus') else "  RSUs (GSUs): N/A")
            print(f"  PSUs: {info.get('psus', 'N/A'):,.0f}" if info.get('psus') else "  PSUs: N/A")
            print(f"  GSUs: {info.get('gsus', 'N/A'):,.0f}" if info.get('gsus') else "  GSUs: N/A")
            print(f"  Total Compensation: ${info.get('total_compensation', 'N/A'):,.0f}" if info.get('total_compensation') else "  Total Compensation: N/A")
        
        print(f"\nDIRECTORS ({data['extraction_metadata']['total_directors']}):")
        print("-" * 50)
        for name, info in data['directors'].items():
            print(f"\n{name}:")
            print(f"  Base Pay: ${info.get('base_pay', 'N/A'):,.0f}" if info.get('base_pay') else "  Base Pay: N/A")
            print(f"  Shares Granted: {info.get('shares_granted', 'N/A'):,.0f}" if info.get('shares_granted') else "  Shares Granted: N/A")
            print(f"  Total Shares Owned: {info.get('total_shares_owned', 'N/A'):,.0f}" if info.get('total_shares_owned') else "  Total Shares Owned: N/A")
            print(f"  Total Compensation: ${info.get('total_compensation', 'N/A'):,.0f}" if info.get('total_compensation') else "  Total Compensation: N/A")

def main():
    """Main execution function."""
    extractor = FocusedCompensationExtractor()
    
    # Extract all data
    results = extractor.extract_all_data()
    
    # Save results
    extractor.save_results(results)
    
    # Print summary
    extractor.print_summary(results)
    
    print(f"\nResults saved to:")
    print(f"  - focused_compensation_results.json (detailed JSON data)")
    print(f"  - executive_compensation.csv (executives only)")
    print(f"  - director_compensation.csv (directors only)")
    print(f"  - combined_compensation.csv (all data in one file)")
    print("="*80)

if __name__ == "__main__":
    main()
