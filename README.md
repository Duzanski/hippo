# Pharmacy Claims Data Processing

This application processes pharmacy claims data to calculate metrics and provide business insights for a pharmacy benefits management system.

## Features

The application implements all 4 requirements from the data engineering challenge:

1. **Data Loading & Validation** - Read and validate pharmacy, claims, and reverts data
2. **Metrics Calculation** - Calculate fills, reverts, average unit price, and total price by NPI/NDC
3. **Chain Recommendations** - Find top 2 chains with lowest average unit prices per drug
4. **Quantity Analysis** - Identify most common quantities prescribed per drug

## Requirements

- Python 3.8+
- pandas>=1.5.0

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
# Run the complete analysis with command line arguments
python3 main.py \
  --pharmacy-dir data/pharmacies \
  --claims-dir data/claims \
  --reverts-dir data/reverts \
  --output metrics_output.json


### Command Line Arguments

- `--pharmacy-dir`: Directory containing pharmacy CSV files (required)
- `--claims-dir`: Directory containing claims JSON files (required)  
- `--reverts-dir`: Directory containing reverts JSON files (required)
- `--output` or `-o`: Output filename for basic metrics (default: metrics_output.json)

The application will process data from the specified directories and generate three output files.

## Output Files

The application generates three JSON output files:

### 1. `metrics_output.json` (Items 1 & 2)
Metrics grouped by pharmacy (NPI) and drug (NDC):
```json
[
    {
        "npi": "0000000000",
        "ndc": "00002323401",
        "fills": 82,
        "reverted": 4,
        "avg_price": 377.56,
        "total_price": 2509345.2
    }
]
```

### 2. `chain_recommendations.json` (Item 3)
Top 2 chains with lowest average unit prices per drug:
```json
[
    {
        "ndc": "00015066812",
        "chain": [
            {
                "name": "health",
                "avg_price": 377.56
            },
            {
                "name": "saint", 
                "avg_price": 413.40
            }
        ]
    }
]
```

### 3. `quantity_analysis.json` (Item 4)
Most common quantities prescribed per drug:
```json
[
    {
        "ndc": "00002323401",
        "most_prescribed_quantity": [
            8.5, 15.0, 45.0, 180.0, 2.0
        ]
    }
]
```

## Data Processing Logic

### Data Validation
- **Pharmacy Data**: Validates NPI and chain fields from CSV files
- **Claims Data**: Validates required fields (id, npi, ndc, price, quantity, timestamp) and data types
- **Reverts Data**: Validates required fields (id, claim_id, timestamp) and timestamp format
- **Filtering**: Only processes claims from valid pharmacies (NPIs in pharmacy dataset)

### Business Logic
- **Unit Price**: Calculated as `price / quantity` for each claim
- **Reverted Claims**: Matched using claim_id from reverts data
- **Chain Analysis**: Groups by NDC and chain, calculates average unit prices
- **Quantity Analysis**: Uses value_counts() to find most common quantities per drug

## Current Implementation Status

✅ **Completed Items:**
- Item 1: Data loading and validation from JSON/CSV files
- Item 2: Metrics calculation (fills, reverts, avg unit price, total price)
- Item 3: Chain recommendations using Pandas (top 2 cheapest chains per drug)
- Item 4: Quantity analysis using Pandas (most common quantities per drug)

## Architecture

The application uses a class-based design with clear separation of concerns:

- `PharmacyDataProcessor`: Main processing class
- **Data Loading**: Separate methods for each data type with validation
- **Analysis Methods**: 
  - Items 1-2: Pure Python with standard library
  - Items 3-4: Pandas DataFrames for advanced analysis
- **Error Handling**: Graceful handling of malformed data with logging

