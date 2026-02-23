# Google Sheets ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline that extracts data from Google Sheets and loads it into both PostgreSQL and MongoDB databases.

## Project Overview

This project demonstrates:
- **Extract**: Reading data from Google Sheets using the Google Sheets API
- **Transform**: Data cleaning, validation, and type conversion
- **Load**: Bulk insertion into PostgreSQL and MongoDB with error handling

## Technologies Used

- **Python 3.12**
- **PostgreSQL** (Aiven Cloud)
- **MongoDB** (Atlas)
- **Google Sheets API**
- **Libraries**: pandas, psycopg2, pymongo, gspread

## Project Structure
google-sheets-etl/
├── etl_pipeline.py          # Main ETL script
├── verify_data.py            # Data verification script
├── test_mongodb.py           # MongoDB connection test
├── requirements.txt          # Python dependencies
├── .env.example             # Example environment variables
├── .gitignore               # Git ignore rules
└── README.md                # Project documentation
##  Setup Instructions

### Prerequisites
- Python 3.8+
- PostgreSQL database
- MongoDB database
- Google Cloud Project with Sheets API enabled

### Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/google-sheets-etl.git

cd google-sheets-etl

Create virtual environment:

python3 -m venv venv
source venv/bin/activate

Install dependencies:
pip install -r requirements.txt

Set up environment variables:
cp .env.example .env
```
# Edit .env with your credentials
Add Google credentials:
Place your credentials.json file in the project root

Running the Pipeline
python3 etl_pipeline.py

Verifying Data
python3 verify_data.py

Features
Bulk data insertion (50,000+ rows in seconds)

Error handling and logging

Data validation and cleaning

Duplicate handling (upsert operations)

Progress tracking Support for UUID identifiers

Author
Joan Matata

