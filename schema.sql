-- schema.sql
-- Dimension Tables
CREATE TABLE CustomerDim (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    gender TEXT,
    age INTEGER,
    country TEXT,
    registration_date DATE
);

CREATE TABLE ProductDim (
    product_id INTEGER PRIMARY KEY,
    name TEXT,
    category TEXT,
    price REAL,
    supplier TEXT
);

CREATE TABLE TimeDim (
    time_id INTEGER PRIMARY KEY,
    date DATE,
    day INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER,
    day_of_week TEXT
);

CREATE TABLE LocationDim (
    location_id INTEGER PRIMARY KEY,
    country TEXT,
    region TEXT,
    city TEXT
);

-- Fact Table
CREATE TABLE SalesFact (
    sale_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    time_id INTEGER,
    location_id INTEGER,
    quantity INTEGER,
    sales_amount REAL,
    discount REAL,
    FOREIGN KEY (customer_id) REFERENCES CustomerDim(customer_id),
    FOREIGN KEY (product_id) REFERENCES ProductDim(product_id),
    FOREIGN KEY (time_id) REFERENCES TimeDim(time_id),
    FOREIGN KEY (location_id) REFERENCES LocationDim(location_id)
);