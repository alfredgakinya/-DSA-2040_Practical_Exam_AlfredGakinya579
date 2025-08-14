# visualization.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def visualize_sales_by_country(db_path='retail_dw.db'):
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT 
        c.country,
        SUM(f.sales_amount) as total_sales
    FROM 
        SalesFact f
    JOIN CustomerDim c ON f.customer_id = c.customer_id
    GROUP BY 
        c.country
    ORDER BY 
        total_sales DESC
    LIMIT 10;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    plt.figure(figsize=(10, 6))
    plt.bar(df['country'], df['total_sales'])
    plt.title('Top 10 Countries by Sales Revenue')
    plt.xlabel('Country')
    plt.ylabel('Total Sales (USD)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('sales_by_country.png')
    plt.close()

if __name__ == "__main__":
    visualize_sales_by_country()