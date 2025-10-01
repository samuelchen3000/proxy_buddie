<<<<<<< HEAD
# Compensation Data Extraction System

## 🚀 Quick Start

```bash
# Extract compensation data from Alphabet proxy tables
python generalized_compensation_extractor.py

# For other companies
python generalized_compensation_extractor.py --company "Apple Inc." --tables-dir "apple_tables"
```

## 📊 What It Does

Extracts executive and director compensation data from proxy statement tables:

**Executives**: Base Pay, Cash Bonus, Stock Awards, RSUs, PSUs, GSUs  
**Directors**: Base Pay, Stock Awards, Common Stock Owned

## 📁 Output Files

- `generalized_compensation_combined.csv` - All data in one file ⭐
- `generalized_compensation_executives.csv` - Executives only
- `generalized_compensation_directors.csv` - Directors only
- `generalized_compensation_results.json` - Detailed JSON with validation

## 🔧 Key Features

- ✅ **Cross-company compatibility** - Works with any proxy format
- ✅ **AI validation** - Uses OpenAI to validate extracted data
- ✅ **Advanced name cleaning** - Removes titles and company references
- ✅ **Automatic table detection** - Finds compensation tables automatically
- ✅ **Multiple output formats** - CSV and JSON

## 📖 Full Documentation

See [COMPENSATION_EXTRACTION_SUMMARY.md](COMPENSATION_EXTRACTION_SUMMARY.md) for complete details.

## 🎯 Results

Successfully extracted compensation data for 6 executives and 9 directors from Alphabet's 2024 proxy statement with 7/10 AI validation confidence score.

---

**Ready for production use!** 🎉
=======
# proxy_buddie
>>>>>>> 0bc01e11bd69a9d1d2dec42aad55938a381a756a
