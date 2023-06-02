import mysql.connector
import boto3
import csv
import os

# MySQL database details
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'root'
database_name = 'bha6'

# AWS S3 details
bucket_name = 'local-mysql-to-s3'  # Replace with your bucket name
key_prefix = 'table-data/'  # Replace with the desired key prefix

# Connect to the MySQL database
conn = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=database_name
)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Create an S3 client
s3 = boto3.client('s3')

# Retrieve table names from the database
cursor.execute("SHOW TABLES")

# Fetch all table names as a list
table_names = [table[0] for table in cursor]

# Iterate over table names
for table_name in table_names:
    # Create a CSV file name based on the table name
    csv_file_name = f"{table_name}.csv"

    # Retrieve column names from the table
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    column_names = [column[0] for column in cursor]

    # Retrieve all rows from the table
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Upload data to S3 as CSV
    with open(csv_file_name, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write column names as the first row
        csv_writer.writerow(column_names)

        # Write rows
        csv_writer.writerows(rows)

    # Upload the CSV file to S3
    s3.upload_file(csv_file_name, bucket_name, f"{key_prefix}{csv_file_name}")

    # Remove the local CSV file
    os.remove(csv_file_name)

# Close the cursor
cursor.close()

# Close the database connection
conn.close()
