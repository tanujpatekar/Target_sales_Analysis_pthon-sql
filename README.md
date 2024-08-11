# Target Sales Analysis with Python and Sql
<img width="400" alt="Coding" src="https://corporate.target.com/getmedia/d2441ab3-7b0b-4bff-9a6f-15df4690559d/New-Stores_Header_Target.png">

## Project Overview

**Project Title**: Target Sales Analysis with Python and Sql  
**Level**: Advanced  
**Database**: `https://www.kaggle.com/datasets/devarajv88/target-dataset?select=products.csv`

This project is designed to demonstrate SQL skills and techniques typically used by data analysts to explore, clean, and analyze sales data. The project involves setting up a Target sales database which has 1 Lakh rows of data with multiple tables in it, performing exploratory data analysis (EDA), and answering specific business questions through SQL queries.

## Objectives
1. **Connecting Database**: Connecting Jupyter Notebook to MySQL Database with the help of Python code.
1. **Set up a Target sales database**: Create and populate a Target sales database with the provided sales data.
2. **Data Cleaning**: Identify and remove any records with missing or null values.
3. **Exploratory Data Analysis (EDA)**: Perform basic exploratory data analysis to understand the dataset.
4. **Business Analysis**: Use SQL to answer specific business questions and derive insights from the sales data.

## Project Structure

### 1. Creating Database Setup in MySql

- **Database Creation**: The project starts by creating a database named `ecommerce` in MySql.
```sql
CREATE DATABASE ecommerce;

```
### 2. Connecting Jupyter Notebook to MySQL Database and Cleaning the data with the help of Python code

- **Connecting Database**: After creating a database in MySql, connect it to Jupyter Notebook. As the data is huge populating it on MySQL will take lot of time, thats why we use a Python code to make this task easy.
- **Cleaning Database**: This code also cleans the data after succesfully connecting the Database .

```sql
import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('payments.csv', 'payments'),
    ('order_items.csv', 'order_items')# Added payments.csv for specific handling
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='analyst',
    database='ecommerce'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'C:/Users/91810/Desktop/Ecommerce/'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)

```

### 2. Data Exploration & Cleaning

- **Replacing missing values**:  Replace NaN with None to handle SQL NULL.
- **Debugging**: Check for NaN values.
- **Null Value Check**: Check for any null values in the dataset and delete records with missing data.

```sql
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()
```
### 3. Final connection

- **Connection Code**: This code makes you write code in Jupyter Notebook and communicate it with MySQL .
```sql
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
import numpy as np

db = mysql.connector.connect(host= "localhost",username = "root",password = "analyst",database = "ecommerce")
cur =db.cursor()

```

### 4. Data Analysis & Findings

  <h1><a name="identifyingnullvaluesinthedataset">1. List all unique cities where customers are located</a></h1>

```python
query = """ select distinct customer_city from customers
limit 5"""

cur.execute(query)

data = cur.fetchall()

data
```
 <h1><a name="identifyingnullvaluesinthedataset">2. Count the number of orders placed in 2017.</a></h1>

```python
query = """ select count(order_id) from orders where 
year(order_purchase_timestamp) = 2017"""

cur.execute(query)

data = cur.fetchall()

"total orders placed in 2017",data[0][0]
```
 <h1><a name="identifyingnullvaluesinthedataset">3. Find the total sales per category.</a></h1>

```python
query = """ select upper(products.product_category) category,round(sum(payments.payment_value),2) sales
from products join order_items on products.product_id = order_items.product_id
join payments on payments.order_id = order_items.order_id
group by category order by Sales Desc
"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns=["Category","Sales"])
df
```
 <h1><a name="identifyingnullvaluesinthedataset">4. Calculate the percentage of orders that were paid in installments.
</a></h1>

```python
query = """ select (sum(case when payment_installments >= 1 then 1 else 0 end))/count(*)*100
from payments"""

cur.execute(query)

data = cur.fetchall()

"the percentage of orders that were paid in installments is",data[0][0]
```
 <h1><a name="identifyingnullvaluesinthedataset">5. Count the number of customers from each state. 
</a></h1>

```python
query = """ select customer_state, count(customer_id) from customers
group by customer_state"""

cur.execute(query)

data = cur.fetchall()
df = pd.DataFrame(data,columns = ["State","customer_count"])
plt.bar(df["State"],df["customer_count"])
plt.show()
```

 <h1><a name="identifyingnullvaluesinthedataset">6. Calculate the number of orders per month in 2018.</a></h1>

```python
query = """ select monthname(order_purchase_timestamp) months,count(order_id) order_count from orders
where year(order_purchase_timestamp) = 2018
group by months
"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["months","order_count"])
o = ["January", "February", "March", "April", "May", "June", 
    "July", "August", "September", "October", "November", "December"]
ax=sns.barplot(x = df["months"],y = df["order_count"],data = df,order = o )
plt.xticks(rotation = 45)
ax.bar_label(ax.containers[0])
plt.title("Count of orders by month in year 2018")
plt.show()
```
 <h1><a name="identifyingnullvaluesinthedataset">7. Find the average number of products per order, grouped by customer city.
</a></h1>

```python
query = """ with count_per_order as
(
    select orders.order_id, orders.customer_id, count(order_items.order_id) as oc
    from orders join order_items
    on orders.order_id = order_items.order_id
    group by orders.order_id, orders.customer_id
)

select customers.customer_city, round(avg(count_per_order.oc), 2) average_orders
from customers join count_per_order
on customers.customer_id = count_per_order.customer_id
group by customers.customer_city order by average_orders desc
;"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns=["City","Average Orders"])
df.head(10)
```
 <h1><a name="identifyingnullvaluesinthedataset">8. Calculate the percentage of total revenue contributed by each product category.
</a></h1>

```python
query = """select upper(products.product_category) category,round((sum(payments.payment_value)/(select sum(payment_value) from payments))*100,2) sales_percentage
from products join order_items on products.product_id = order_items.product_id
join payments on payments.order_id = order_items.order_id
group by category order by sales_percentage Desc """

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns=["Category","Sales_percentage"])
df.head(5)
```
 <h1><a name="identifyingnullvaluesinthedataset">9. Identify the correlation between product price and the number of times a product has been purchased.
</a></h1>

```python

query = """SELECT products.product_category,
count(order_items.product_id),round(avg(order_items.price),2)
from products join 
order_items on products.product_id = order_items.product_id
group by products.product_category;
"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns=["Category","order_count","price"])
df.head(5)
arr1 = df["order_count"]
arr2 = df["price"]
a = np.corrcoef([arr1,arr2])
print("the correlation is",a[0][-1])
```
 <h1><a name="identifyingnullvaluesinthedataset">10. Calculate the total revenue generated by each seller, and rank them by revenue.
</a></h1>

```python
query = """select *,dense_rank() over(order by revenue desc) as rn from 
(select order_items.seller_id, round(sum(payments.payment_value),2) revenue
from order_items join payments
on order_items.order_id =payments.order_id
group by order_items.seller_id) as a"""

cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data,columns=["Seller_id","Revenue","Rank"])
df = df.head()
sns.barplot(x = "Seller_id", y = "Revenue",data = df)
plt.xticks(rotation = 90)
plt.show
```
 <h1><a name="identifyingnullvaluesinthedataset">11. Calculate the moving average of order values for each customer over their order history.

</a></h1>

```python
query = """select customer_id, order_purchase_timestamp,payment,
avg(payment) over(partition by customer_id order by order_purchase_timestamp rows between 2 preceding and current row) as mov_avg
from
(select orders.customer_id, orders.order_purchase_timestamp, payments.payment_value as payment
from payments join orders
on payments.order_id = orders.order_id)as a;"""

cur.execute(query)

data = cur.fetchall()
df = pd.DataFrame(data,columns=["customer_id","order_purchase_timestamp","payment","mov_avg"])
df
```
 <h1><a name="identifyingnullvaluesinthedataset">12. Calculate the cumulative sales per month for each year.

</a></h1>

```python
query = """ select distinct customer_city from customers
query = """select years, months, payment,sum(payment)
over(order by years, months) cumulative_sales from
(select year(orders.order_purchase_timestamp) as years,
month(orders.order_purchase_timestamp) as months,
round(sum(payments.payment_value),2) as payment from orders join payments
on orders.order_id = payments.order_id
group by years, months order by years, months) as a;"""

cur.execute(query)

data = cur.fetchall()
df = pd.DataFrame(data,columns=["year","month","payment","cum_sales"])
df
```
 <h1><a name="identifyingnullvaluesinthedataset">13. Calculate the year-over-year growth rate of total sales.

</a></h1>

```python
query = """with a as (select year(orders.order_purchase_timestamp) as years,
round(sum(payments.payment_value),2) as payment from orders join payments
on orders.order_id = payments.order_id
group by years order by years)
select years,((payment-lag(payment,1) over(order by years))/ lag(payment,1) over(order by years)) * 100 from a"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data,columns = ["years","yoy%growth"])
df
```
 <h1><a name="identifyingnullvaluesinthedataset">14. Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

</a></h1>

```python
query = """with a as (select customers.customer_id,
min(orders.order_purchase_timestamp) first_order
from customers join orders
on customers.customer_id = orders.customer_id
group by customers.customer_id),

b as (select a.customer_id, count(distinct orders.order_purchase_timestamp) next_order
from a join orders
on orders.customer_id = a.customer_id
and orders.order_purchase_timestamp > first_order
and orders.order_purchase_timestamp <
date_add(first_order, interval 6 month)
group by a.customer_id)

select 100 * (count( distinct a.customer_id)/ count(distinct b.customer_id))
from a left join b
on a.customer_id = b.customer_id ;"""
cur.execute(query)
data = cur.fetchall()
data
```
 <h1><a name="identifyingnullvaluesinthedataset">15. Identify the top 3 customers who spent the most money in each year.

```python
query = """select years, customer_id, payment, d_rank
from
(select year(orders.order_purchase_timestamp) years,
orders.customer_id,
sum(payments.payment_value) payment,
dense_rank() over(partition by year(orders.order_purchase_timestamp)
order by sum(payments.payment_value) desc) d_rank
from orders join payments
on payments.order_id = orders.order_id
group by year(orders.order_purchase_timestamp),
orders.customer_id) as a
where d_rank <= 3 ;"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data,columns = ["years","id","payment","rank"])
sns.barplot(x = "id", y = "payment",data = df,hue = "years")
plt.xticks(rotation = 90)
plt.show()
```
## Conclusion

This project serves as a comprehensive introduction to SQL for data analysts, covering database setup, data cleaning, exploratory data analysis, and business-driven SQL queries. The findings from this project can help drive business decisions by understanding sales patterns, customer behavior, and product performance.


  
