import psycopg2
from connection import conn, cur

conn.set_session(autocommit=True)

#Delete all tables and make the schema empty.
querydrop = """
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO admin;
GRANT ALL ON SCHEMA public TO public;
COMMENT ON SCHEMA public IS 'standard public schema';
"""
cur.execute(querydrop)
print("All tables are deleted successfully")
print("-----------------------------------")
print("-----------------------------------")


#Query1: Create Dim_Store table.
query1 = """
CREATE TABLE Dim_Store
(
    id SERIAL NOT NULL PRIMARY KEY,
    store integer NOT NULL
)
"""
cur.execute(query1)
print("Table -> Dim_Store is created successfully")

#Query2: Create Dim_Date table.
query2 = """
CREATE TABLE Dim_Date
(
    date DATE NOT NULL PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
)
"""
cur.execute(query2)
print("Table -> Dim_Date is created successfully")

#Query3: Create Dim_Item table.
query3 = """
CREATE TABLE Dim_Item
(
    id SERIAL NOT NULL PRIMARY KEY,
    item integer NOT NULL
)
"""
cur.execute(query3)
print("Table -> Dim_Item is created successfully")

#Query4: Create Fact_Sales table.
query4 = """
CREATE TABLE Fact_Sales
(
    store_id SERIAL NOT NULL REFERENCES Dim_Store (id),
    date_id date NOT NULL REFERENCES Dim_Date (date),
    item_id SERIAL NOT NULL REFERENCES Dim_Item (id),
    sales FLoat,
    sales_predicted FLoat,
    sales_predicted_upper FLoat,
    sales_predicted_lower FLoat
)
"""
cur.execute(query4)
print("Table -> Fact_Sales is created successfully")


# Close the cursor
cur.close()

# Close the connection
conn.close()
