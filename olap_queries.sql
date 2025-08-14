-- olap_queries.sql
-- Query 1: Roll-up - Total sales by country and quarter
SELECT 
    c.country,
    t.quarter,
    t.year,
    SUM(f.sales_amount) as total_sales
FROM 
    SalesFact f
JOIN CustomerDim c ON f.customer_id = c.customer_id
JOIN TimeDim t ON f.time_id = t.time_id
GROUP BY 
    c.country, t.quarter, t.year
ORDER BY 
    c.country, t.year, t.quarter;

-- Query 2: Drill-down - Sales details for UK by month
SELECT 
    strftime('%Y-%m', t.date) as month,
    SUM(f.sales_amount) as total_sales,
    COUNT(DISTINCT f.customer_id) as unique_customers,
    SUM(f.quantity) as total_quantity
FROM 
    SalesFact f
JOIN CustomerDim c ON f.customer_id = c.customer_id
JOIN TimeDim t ON f.time_id = t.time_id
WHERE 
    c.country = 'United Kingdom'
GROUP BY 
    strftime('%Y-%m', t.date)
ORDER BY 
    month;

-- Query 3: Slice - Total sales for electronics category
SELECT 
    SUM(f.sales_amount) as total_sales,
    AVG(f.sales_amount) as avg_sale,
    COUNT(*) as transaction_count
FROM 
    SalesFact f
JOIN ProductDim p ON f.product_id = p.product_id
WHERE 
    p.category = 'Electronics';