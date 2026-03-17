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


import json

import pandas as pd  # import pandas library for data load and transform data(columns rename,drop column,null/missing value handle by median or mean,convert data type,groupby and merge, etc.)

import mysql.connector as connector  # import mysql.connector library for connect to mysql database and perform sql query


# In[ ]:


def DB_Config(dbtype):

    print("Reading settings.json file for connection details...")

    # 1. Simple assignment (most common)
    read_file = None
    settings_path = '.vscode/settings.json'
    dbconfig = None      

    # 2. Multiple assignment
    host, port, dbname, user, password = None, None, None, None, None

    # 3. Default values    
    settings = {}
    connections = None

    try:
         if not dbtype.lower() in ['postgresql','mysql']:
            print(f"Database type '{dbtype}' is not recognized. No data transfer will be performed.") # print a message indicating that the provided database type is not recognized and that no data transfer will be performed.           
            return dbconfig   

         if not settings_path is '': 
            read_file = open(settings_path, 'r') # open the settings.json file in read mode to access the connection settings.
         else:    
            print(f"No settings file found. Please check your settings.json.")
            return dbconfig

         if not read_file is None:            
            settings = json.load(read_file) # load the contents of the settings.json file into a Python dictionary to access the connection settings.
         else:    
            print(f"No connections found. Please check your settings.json.")
            return dbconfig

         if not settings is None:
            connections = settings.get('sqltools.connections',None) # retrieve the list of database connections from the settings dictionary using the key 'sqltools.connections' to access the connection settings.
         else:    
            print(f"No connections found. Please check your settings.json.")
            return dbconfig

         if not connections is None:           

            if dbtype.lower() =='postgresql':
               dbconfig = next((c for c in connections 
                       if c['driver'] == 'PostgreSQL' and c['database'] == 'customer_behavior'), None) # use a list comprehension to find the first PostgreSQL connection in the list of connections that has a driver of 'PostgreSQL' and a database name of 'customer_behavior'.

               print(f"Found PostgreSQL connection settings. Server:- {dbconfig['server']} ' Port:- ' {dbconfig.get('port', 5432)} ' Database:- ' {dbconfig['database']} ' Username:- ' {dbconfig['username']} ' Password:- ' {dbconfig['password']}") # print the server and database name from the PostgreSQL connection settings to verify that the correct connection details have been retrieved from the settings.json file.       

            elif dbtype.lower() =='mysql':
               dbconfig = next((c for c in connections 
                       if c['driver'] == 'MySQL' and c['database'] == 'customer_behavior'), None) # use a list comprehension to find the first MySQL connection in the list of connections that has a driver of 'MySQL' and a database name of 'customer_behavior'.

               print(f"Found MySQL connection settings. Server:- {dbconfig['server']} ' Port:- ' {dbconfig.get('port', 3306)} ' Database:- ' {dbconfig['database']} ' Username:- ' {dbconfig['username']} ' Password:- ' {dbconfig['password']}") # print the server and database name from the MySQL connection settings to verify that the correct connection details have been retrieved from the settings.json file.       

            return dbconfig      

         else:    
            print(f"No connections found. Please check your settings.json.")
            return dbconfig

    except Exception as e:
        print(f"Connection failed: {e}")
        return dbconfig
    finally:
        if not read_file is None:
            read_file.close()


# In[ ]:


def process_using_MySql_Connector():   

   dbconfig=None;
   conn=None; 

   try:
          dbconfig=DB_Config("MySQL") # call the DB_Config function with the argument "MySQL" to retrieve the MySQL connection settings from the settings.json file and store them in the variable dbconfig.;

          if not dbconfig is None:
               print(f"Connecting to MySQL database using MySQL Connector with the following settings: Server:- {dbconfig['server']} ' Port:- ' {dbconfig.get('port', 3306)} ' Database:- ' {dbconfig['database']} ' Username:- ' {dbconfig['username']} ' Password:- ' {dbconfig['password']}") # print the MySQL connection settings to verify that the correct connection details have been retrieved from the settings.json file.

               conn = connector.connect(
                                        host=dbconfig['server'],
                                        port=dbconfig.get('port', 3306),
                                        database=dbconfig['database'],
                                        user=dbconfig['username'],
                                        password=dbconfig['password']
                                       ) # establish a connection to the MySQL database using the mysql.connector library and the connection settings retrieved from the settings.json file.

               print(conn) # print the connection object to verify that the connection to the MySQL database has been established successfully.

               if conn.is_connected():
                    print("Connected to MySQL database using MySQL Connector.") # You can add code here to transfer data from the DataFrame to the MySQL database using the connection object.

               else:
                  print(f"No MySQL connection settings found. Please check your settings.json.")

          else:
               print(f"No MySQL connection settings found. Please check your settings.json.")


   except Exception as e:
        print(f"Error connecting to MySQL database: {e}")

   finally:
        if 'connection' in locals() and conn.is_connected():
            conn.close()
            print("MySQL connection closed.")


# In[ ]:


process_using_MySql_Connector() # call the function to transfer data from Python to MySQL database using MySQL Connector, which is an alternative method for connecting to MySQL databases and transferring data, providing flexibility in how the data transfer is performed. 


# In[ ]:




