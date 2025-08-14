# etl_retail.py
import pandas as pd
import sqlite3
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data(file_path):
    """Extract data from CSV file"""
    try:
        logger.info(f"Extracting data from {file_path}")
        df = pd.read_csv(file_path, encoding='unicode_escape')
        logger.info(f"Extracted {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error in extraction: {str(e)}")
        raise

def transform_data(df):
    """Transform the raw data"""
    logger.info("Starting data transformation")
    
    # Handle missing values
    df = df.dropna(subset=['CustomerID'])
    df['CustomerID'] = df['CustomerID'].astype(int)
    
    # Convert date
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    
    # Calculate total sales
    df['TotalSales'] = df['Quantity'] * df['UnitPrice']
    
    # Filter valid data
    df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]
    
    # Filter last year of data (assuming current date is 2025-08-12)
    last_year = df[df['InvoiceDate'] >= '2024-08-12']
    
    logger.info(f"Transformed data. {len(last_year)} rows after filtering")
    return last_year

def create_dimension_tables(conn, df):
    """Create and populate dimension tables"""
    logger.info("Creating dimension tables")
    
    # Customer Dimension
    customer_dim = df.groupby('CustomerID').agg(
        country=('Country', 'first')
    ).reset_index()
    customer_dim['customer_id'] = customer_dim['CustomerID']
    customer_dim['name'] = 'Customer_' + customer_dim['CustomerID'].astype(str)
    customer_dim.to_sql('CustomerDim', conn, if_exists='replace', index=False)
    
    # Time Dimension
    df['time_id'] = df['InvoiceDate'].dt.strftime('%Y%m%d').astype(int)
    time_dim = pd.DataFrame({
        'time_id': df['time_id'].unique(),
        'date': pd.to_datetime(df['time_id'].unique(), format='%Y%m%d'),
        'day': pd.to_datetime(df['time_id'].unique(), format='%Y%m%d').day,
        'month': pd.to_datetime(df['time_id'].unique(), format='%Y%m%d').month,
        'quarter': pd.to_datetime(df['time_id'].unique(), format='%Y%m%d').quarter,
        'year': pd.to_datetime(df['time_id'].unique(), format='%Y%m%d').year,
        'day_of_week': pd.to_datetime(df['time_id'].unique(), format='%Y%m%d').day_name()
    })
    time_dim.to_sql('TimeDim', conn, if_exists='replace', index=False)
    
    logger.info("Dimension tables created")

def create_fact_table(conn, df):
    """Create and populate fact table"""
    logger.info("Creating fact table")
    
    # Join with dimension tables to get foreign keys
    fact_df = df[['InvoiceNo', 'StockCode', 'Quantity', 'UnitPrice', 'TotalSales', 'CustomerID', 'time_id', 'Country']]
    fact_df = fact_df.rename(columns={
        'StockCode': 'product_id',
        'CustomerID': 'customer_id'
    })
    
    # Get location_id (simple mapping for this example)
    fact_df['location_id'] = fact_df['Country'].factorize()[0] + 1
    
    fact_df.to_sql('SalesFact', conn, if_exists='replace', index=False)
    logger.info(f"Fact table created with {len(fact_df)} rows")

def load_data(df, db_path='retail_dw.db'):
    """Load data into SQLite database"""
    try:
        conn = sqlite3.connect(db_path)
        
        # Create tables
        create_dimension_tables(conn, df)
        create_fact_table(conn, df)
        
        conn.close()
        logger.info("Data loading completed successfully")
    except Exception as e:
        logger.error(f"Error in loading data: {str(e)}")
        raise

def etl_pipeline(input_file, output_db):
    """Complete ETL pipeline"""
    try:
        # Extract
        raw_data = extract_data(input_file)
        
        # Transform
        transformed_data = transform_data(raw_data)
        
        # Load
        load_data(transformed_data, output_db)
        
        logger.info("ETL process completed successfully")
        return True
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        return False

if __name__ == "__main__":
    # Run the ETL pipeline
    etl_pipeline('OnlineRetail.csv', 'retail_dw.db')