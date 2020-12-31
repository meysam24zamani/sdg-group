import psycopg2

DB_NAME = "postgres"
DB_USER = "admin"
DB_PASS = "admin"
DB_HOST = "localhost"
DB_PORT = "6000"



try: 
    conn = psycopg2.connect(database = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT)

    print("Database connect successfully...")

except:
    print("Database not connected")

cursor = conn.cursor()

