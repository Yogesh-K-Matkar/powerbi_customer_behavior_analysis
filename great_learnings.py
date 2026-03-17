#!/usr/bin/env python
# coding: utf-8

# ## Packages installed 
# # 1. Python install through  https://www.python.org/downloads/ , then check version through terminal - pip python --version    
# # 2. Pip Python package manager comes with Python installation, check version through terminal - pip --version , update pip through terminal - python -m pip install --upgrade pip
# # 3. Setup interpreter Ctrl+Shift P - Python: Select Interpreter - select the one with Python 3.11.4
# # 4. Jupyter notebook is environment for Python , through terminal- pip install notebook   
# # 5. Pandas provides data structures and data analysis tools for Python, through terminal - pip install pandas
# # 6. Numpy provides support for large, multi-dimensional arrays and matrices, data computation tools, install terminal - pip install numpy
# # 7. tzdata provides time zone data for Python, through terminal - pip install tzdata
# 
# 
# ## To transfer data from Python to a PostgreSQL database, you can use the following steps:
# # 1. Install the required package for PostgreSQL connection (e.g., psycopg2 and sqlalchemy) through terminal - !pip install sqlalchemy  and !pip install psycopg2-binary
# # 2. Import the necessary libraries for database connection and data transfer (e.g., psycopg2, sqlalchemy).

# In[ ]:


import pandas as pd  # import pandas library for data load and transform data(columns rename,drop column,null/missing value handle by median or mean,convert data type,groupby and merge, etc.)

import mysql.connector as connector  # import mysql.connector library for connect to mysql database and perform sql query


# In[ ]:


def process_using_MySql_Connector():   

   conn=None; 

   try:
        conn = connector.connect(
            host='localhost',
            database='customer_behavior',
            user='your_username',
            password='your_password'
        )

        if conn.is_connected():
            print("Connected to MySQL database using MySQL Connector.")
            # You can add code here to transfer data from the DataFrame to the MySQL database using the connection object.

   except Error as e:
        print(f"Error connecting to MySQL database: {e}")

   finally:
        if 'connection' in locals() and conn.is_connected():
            conn.close()
            print("MySQL connection closed.")


# In[ ]:


process_using_MySql_Connector() # call the function to transfer data from Python to MySQL database using MySQL Connector, which is an alternative method for connecting to MySQL databases and transferring data, providing flexibility in how the data transfer is performed. 

