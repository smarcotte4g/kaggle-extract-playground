# kaggle-extract-playground

This is a test to understand the Kaggle API

# Automated Data Pipeline and Reporting Solution

This project automates the data pipeline and reporting process for the `supermarket_sales.csv` dataset using Google Cloud and Python. The solution includes data ingestion, transformation, and real-time reporting with a focus on scalability and automation.

## Features
- Automates the ingestion of raw data from Kaggle.
- Transforms data into a star schema with dimension and fact tables.
- Loads transformed data into a SQLite database (or cloud data warehouse in production).
- Generates analytical reports using SQL with joins and windowing functions.

---

## Prerequisites

To use the Kaggle API, sign up for a Kaggle account at https://www.kaggle.com. Then go to the 'Account' tab of your user profile (https://www.kaggle.com/<username>/account) and select 'Create API Token'. This will trigger the download of kaggle.json, a file containing your API credentials. Place this file in the location appropriate for your operating system.

### 1. Environment Variables
Set up your Kaggle API credentials as environment variables:

- **Linux/Mac**:
```bash
  export KAGGLE_USERNAME="your_kaggle_username"
  export KAGGLE_KEY="your_kaggle_key"
```



### 2. Python Dependencies

Ensure you have Python 3.8+ installed. Install the required packages using the provided requirements.txt file:

```bash
pip install -r requirements.txt
```

### 3. Kaggle Dataset

This project uses the Supermarket Sales dataset from Kaggle. Ensure the dataset is available for download.

## Usage

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd <repository-folder>
```

### Step 2: Run the Data Pipeline

The script performs the following steps:
	1.	Fetches the dataset from Kaggle.
	2.	Transforms it into dimension and fact tables.
	3.	Loads the transformed data into a SQLite database.

Run the script:

```bash
python main.py
```

### Step 3: Analytical Reporting

	1.	Open the SQLite database (supermarket_sales.db) to explore the tables.
	2.	Execute the SQL queries provided in the queries/ folder for reporting.

## Files

### 1. main.py

Main script to:
	•	Download the Kaggle dataset.
	•	Transform the dataset into dimension and fact tables.
	•	Load the data into SQLite.

### 2. requirements.txt

Dependencies for the project:

```bash
pandas
kaggle
sqlite3
```

### 3. queries/report.sql

Contains the SQL query used to generate the analytical report.

### 4. supermarket_sales.db

SQLite database (created after running the script).

## Sample Query

Use the following SQL query to generate an aggregated report:

```sql
SELECT 
    dp.product_line AS product_category,
    dd.year,
    dd.month,
    SUM(fs.total) AS total_sales,
    AVG(fs.gross_income) AS avg_gross_income,
    RANK() OVER (PARTITION BY dd.year, dd.month ORDER BY SUM(fs.total) DESC) AS sales_rank
FROM 
    fact_sales fs
JOIN 
    dim_date dd ON fs.date_id = dd.date_id
JOIN 
    dim_product dp ON fs.product_id = dp.product_id
GROUP BY 
    dp.product_line, dd.year, dd.month
ORDER BY 
    dd.year, dd.month, sales_rank;
```

## Expected Output

| Product Category     | Year | Month | Total Sales | Avg Gross Income | Sales Rank |
|-----------------------|------|-------|-------------|------------------|------------|
| Electronics           | 2023 | 01    | 10000       | 500              | 1          |
| Clothing              | 2023 | 01    | 8000        | 400              | 2          |
| Home Appliances       | 2023 | 01    | 6000        | 300              | 3          |

## Contribution

Feel free to fork the project, submit issues, or create pull requests to enhance the functionality.
