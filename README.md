# -DSA-2040_Practical_Exam_AlfredGakinya579
retail_data_warehouse/
│
├── schema/                # Data warehouse design files
│   ├── retail_star_schema.png  # Star schema diagram
│   └── schema.sql         # SQL table creation scripts
│
├── etl/                   # ETL process implementation
│   ├── etl_retail.py      # Python ETL pipeline
│   └── OnlineRetail.csv   # Sample dataset
│
├── analysis/              # OLAP analysis components
│   ├── olap_queries.sql   # Analytical SQL queries
│   ├── visualization.py   # Data visualization script
│   ├── sales_by_country.png  # Sample visualization
│   └── README.md          # Analysis report
│
└── retail_dw.db           # SQLite database file
1. Data Warehouse Design
Star Schema: Implemented with one fact table (SalesFact) and four dimension tables (CustomerDim, ProductDim, TimeDim, LocationDim)

Schema Choice: I chose star schema over snowflake because:

Simpler queries with fewer joins, better for analytical queries

Better performance for read-heavy operations common in retail analytics

Easier to understand and maintain for business users

SQL Implementation: Complete CREATE TABLE statements for all tables

2. ETL Process
Extraction: Reads retail transaction data from CSV

Transformation:

Handles missing values and data type conversion

Calculates derived metrics (TotalSales = Quantity × UnitPrice)

Filters for valid transactions and recent data

Loading: Populates SQLite database with dimension and fact tables

Logging: Tracks progress and row counts at each stage

3. OLAP Analysis
Three Key Queries:

Roll-up: Sales by country and quarter

Drill-down: UK sales by month

Slice: Electronics category sales

Visualization: Bar chart of top countries by sales revenue

Insights: Identified sales patterns, seasonal trends, and product performance

Implementation Details
Technical Stack
Database: SQLite

Programming Language: Python

Libraries: pandas, sqlite3, matplotlib

Data: UCI ML Online Retail dataset (or synthetic alternative)

Key Features
Complete end-to-end data pipeline from raw data to insights

Robust error handling and data validation

Dimensionally modeled for analytical queries

Automated ETL process with logging
