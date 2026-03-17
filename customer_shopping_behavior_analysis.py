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

# In[1]:


import pandas as pd  # import pandas library for data load and transform data(columns rename,drop column,null/missing value handle by median or mean,convert data type,groupby and merge, etc.)


# In[2]:


try:
    df=pd.read_csv('doc/customer_shopping_behavior.csv'); # read csv file and store in variable 

except Exception as e:
    print(f"Error reading the CSV file: {e}") # print an error message if there is an issue reading the CSV file, such as an incorrect file path or a missing file. else: # Runs if no exception print("CSV file read successfully.") # print a success message if the CSV file is read successfully. finally: print("Final block executed.") # print a message indicating that the final block of code has been executed, which runs regardless of whether an exception occurred or not.    

else:
    csv_rows_count=df.shape[0] # get number of rows in the DataFrame to understand the size of the dataset and how many customer records are available for analysis.

    csv_columns_count=df.shape[1] # get number of columns in the DataFrame to understand the number of features or attributes available for each customer record in the dataset.

finally:
    print(csv_rows_count) # display the number of rows in the DataFrame to verify that the dataset has been loaded correctly and to get an overview of the amount of data available for analysis.

    print(csv_columns_count) # display the number of columns in the DataFrame to verify that all expected columns are present in the dataset. 


# In[3]:


df.head() # display first 5 rows of the dataset


# In[4]:


df.info() # display summary(no of rows and column count and data types) of the dataset, including data types and non-null counts


# In[5]:


df.describe() # display statistical summary of numerical columns of the dataset, including count, mean, std, min, 25%, 50%, 75%, and max values for each numeric column


# In[6]:


df.describe(include='all') # display statistical summary of all (numerical and categorical) columns of the dataset, including count, unique, top, freq for categorical columns and count, mean, std, min, 25%, 50%, 75%, and max values for numeric columns


# In[7]:


df.isnull().sum() # check for missing values in each column of the dataset and display the count of missing values for each column


# In[8]:


df.columns=df.columns.str.lower() # convert all column names to lowercase for consistency and easier access in code, allowing you to reference columns without worrying about case sensitivity.


# In[9]:


df.columns=df.columns.str.replace(' ', ' ') # replace multiple spaces in column names with single spaces before replacing spaces with underscores, ensuring that column names are clean and consistent for easier access in code, especially when referencing columns with spaces in their names.


# In[10]:


df.columns=df.columns.str.replace(' ', '_') # replace spaces in column names with underscores for easier access and to avoid issues when referencing columns in code, allowing you to use column names without spaces.



# In[11]:


df=df.rename(columns={'purchase_amount_(usd)':'purchase_amount'},inplace=False) # rename the 'purchase_amount_(usd)' column to 'purchase_amount' for consistency with the lowercase column names and to avoid issues when referencing the column in code, ensuring that all column names follow a consistent naming convention.
# inplace=False ensures that the original DataFrame is not modified in place, and instead returns a new DataFrame with the renamed column, which is then assigned back to the variable df to update the DataFrame with the new column name.

df.columns # display the column names of the dataset to verify that they have been converted to lowercase.


# In[12]:


df['review_rating']=df.groupby('category')['review_rating'].transform(lambda x: x.fillna(x.mean())) # calculate the mean review rating for each product and assign it back to the 'Review Rating' column in the original DataFrame, effectively replacing individual ratings with the average rating for each product.

df.isnull().sum() # check for missing values again to confirm that the 'Review Rating' column no longer has any missing values after filling them with the mean ratings.


# In[13]:


# create new column 'age_group' based on 'age' column to categorize customers into age groups (e.g., 18-25, 26-35, etc.) for better analysis of purchase trends by age group.

age_groups_labels=['Young Adult','Adult','Middle-aged','Senior'] # define age group labels for categorizing customers into age groups based on their age.

df['age_group']=pd.qcut(df['age'], q=4, labels=age_groups_labels) # use pd.qcut to create age groups based on the 'age' column, dividing the customers into four equal groups (quartiles) and assigning the corresponding age group labels defined in 'age_groups_labels' to the new 'age_group' column.


# In[14]:


df[['age','age_group']].head(csv_rows_count) # display the 'age' and 'age_group' columns to verify that the age groups have been created correctly based on the age values in the dataset.


# In[15]:


# create purchase frequency days column to analyze how frequently customers make purchases and identify trends in purchase behavior over time.

purchase_frequency_mapping={"Fortnightly":14,"Weekly":7,"Annually":365,"Quarterly":91,"Bi-Weekly":14,"Monthly":30,"Every 3 Months":90} # define a mapping of purchase frequency categories to their corresponding number of days, which will be used to create a new column that quantifies the purchase frequency in terms of days.

df["purchase_frequency_days"] = df["frequency_of_purchases"].map(purchase_frequency_mapping)

df[["frequency_of_purchases","purchase_frequency_days"]].head(csv_rows_count) # display the 'frequency_of_purchases' and 'purchase_frequency_days' columns to verify that the purchase frequency has been correctly mapped to the corresponding number of days based on the defined mapping.

df.columns


# In[16]:


 # remove not necessary column with unique values but 2 different columns with same values and rename the column to avoid confusion and redundancy in the dataset, ensuring that the dataset is clean and easier to analyze without duplicate or redundant columns.


(df['discount_applied']==df['promo_code_used']).all() # check if the 'discount_applied' and 'promo_code_used' columns have the same values for all rows, which would indicate that they are redundant and one of them can be removed from the dataset to avoid confusion and redundancy in analysis.

df = df.drop('promo_code_used',axis=1) # remove the redundant 'promo_code_used' column since it has the same values as 'discount_applied', ensuring that the dataset is clean and easier to analyze without duplicate or redundant columns.
# Axis specifies that we want to drop a column (axis=1) rather than a row (axis=0).

df.columns


# In[17]:


# Data Transfer Process from Python to PostgreSQL database

#  1. Connect to PostgreSQL using SQLTools settings.json

import json


def DB_Config(dbtype):

    print("Connecting to PostgreSQL database...")

    import json
    # import psycopg2

    # 1. Simple assignment (most common)
    read_file = None
    settings_path = '.vscode/settings.json'
    pg_conn = None      

    # 2. Multiple assignment
    host, port, dbname, user, password = None, None, None, None, None

    # 3. Default values    
    settings = {}
    connections = None

    try:
         if not dbtype.lower() in ['postgresql','mysql']:
            print(f"Database type '{dbtype}' is not recognized. No data transfer will be performed.") # print a message indicating that the provided database type is not recognized and that no data transfer will be performed.           
            return pg_conn   

         if not settings_path is '': 
            read_file = open(settings_path, 'r') # open the settings.json file in read mode to access the connection settings.
         else:    
            print(f"No settings file found. Please check your settings.json.")
            return pg_conn

         if not read_file is None:            
            settings = json.load(read_file) # load the contents of the settings.json file into a Python dictionary to access the connection settings.
         else:    
            print(f"No connections found. Please check your settings.json.")
            return pg_conn

         if not settings is None:
            connections = settings.get('sqltools.connections',None) # retrieve the list of database connections from the settings dictionary using the key 'sqltools.connections' to access the connection settings.
         else:    
            print(f"No connections found. Please check your settings.json.")
            return pg_conn

         if not connections is None:           

            if dbtype.lower() =='postgresql':
               pg_conn = next((c for c in connections 
                       if c['driver'] == 'PostgreSQL' and c['database'] == 'customer_behavior'), None) # use a list comprehension to find the first PostgreSQL connection in the list of connections that has a driver of 'PostgreSQL' and a database name of 'customer_behavior'.

               print(f"Found PostgreSQL connection settings. Server:- {pg_conn['server']} ' Port:- ' {pg_conn.get('port', 5432)} ' Database:- ' {pg_conn['database']} ' Username:- ' {pg_conn['username']} ' Password:- ' {pg_conn['password']}") # print the server and database name from the PostgreSQL connection settings to verify that the correct connection details have been retrieved from the settings.json file.       

            elif dbtype.lower() =='mysql':
               pg_conn = next((c for c in connections 
                       if c['driver'] == 'MySQL' and c['database'] == 'customer_behavior'), None) # use a list comprehension to find the first MySQL connection in the list of connections that has a driver of 'MySQL' and a database name of 'customer_behavior'.

               print(f"Found MySQL connection settings. Server:- {pg_conn['server']} ' Port:- ' {pg_conn.get('port', 3306)} ' Database:- ' {pg_conn['database']} ' Username:- ' {pg_conn['username']} ' Password:- ' {pg_conn['password']}") # print the server and database name from the MySQL connection settings to verify that the correct connection details have been retrieved from the settings.json file.       

            return pg_conn

         else:    
            print(f"No connections found. Please check your settings.json.")
            return pg_conn

    except Exception as e:
        print(f"Connection failed: {e}")
        return pg_conn
    finally:
        if not read_file is None:
            read_file.close()


# In[18]:


# 2. Transfer data DATAFRAME to PostgreSQL database

def DBConnect_PostgreSQL(df, table_name, db_conn):

    conn_str=None
    conn=None
    engine=None # initialize a variable to hold the database connection engine, which will be used to establish a connection to the PostgreSQL database and transfer data from the DataFrame to the database.    
    dt=None

    try:    


        if not db_conn is None:

            conn_str = f"postgresql+psycopg2://{db_conn['username']}:{db_conn['password']}@{db_conn['server']}:{db_conn.get('port', 5432)}/{db_conn['database']}" # construct the connection string for PostgreSQL using the connection details from the db_conn dictionary, which will be used to create a database connection engine for transferring data from the DataFrame to the PostgreSQL database.


        if not conn_str is None:
            from sqlalchemy import create_engine
            engine = create_engine(conn_str) # create a database connection engine using the connection string, which will be used to establish a connection to the PostgreSQL database and transfer data from the DataFrame to the database.

        if not engine is None:
            df.to_sql(table_name, engine, if_exists='replace', index=False) # transfer the DataFrame to the PostgreSQL database using the to_sql method, specifying the target table name and connection engine.
            print(f"Data transferred successfully to table '{table_name}' in PostgreSQL database.") # print a success message if the data transfer is successful.   

            return True # return True to indicate that the data transfer was successful

        else: 
            print("No valid connection engine available. Data transfer to PostgreSQL database failed.") # print a message indicating that there is no valid connection engine available, which means that the data transfer to the PostgreSQL database has failed.            
            return False # return False to indicate that the data transfer was not successful

    except Exception as e:
        print(f"Error transferring data to PostgreSQL database: {e}") # print an error message if there is an issue during the data transfer process, including the exception message for debugging purposes. 

        return False # return False to indicate that there was an error during the data transfer process

    finally:


        if not engine is None:
            engine.dispose() # dispose of the database connection engine to free up resources


def DBConnect_MySQL(df, table_name,db_conn ):

    conn_str=None
    engine=None # initialize a variable to hold the database connection engine, which will be used to establish a connection to the MySQL database and transfer data from the DataFrame to the database.    

    try:    

        if not db_conn is None:
            conn_str = f"mysql+pymysql://{db_conn['username']}:{db_conn['password']}@{db_conn['server']}:{db_conn.get('port', 3306)}/{db_conn['database']}" # construct the connection string for MySQL using the connection details from the db_conn dictionary, which will be used to create a database connection engine for transferring data from the DataFrame to the MySQL database.

        if not conn_str is None:
            from sqlalchemy import create_engine
            engine = create_engine(conn_str) # create a database connection engine using the connection string, which will be used to establish a connection to the MySQL database and transfer data from the DataFrame to the database.

        if not engine is None:
            df.to_sql(table_name, engine, if_exists='replace', index=False) # transfer the DataFrame to the MySQL database using the to_sql method, specifying the target table name and connection engine.
            print(f"Data transferred successfully to table '{table_name}' in MySQL database.") # print a success message if the data transfer is successful.
            return True # return True to indicate that the data transfer was successful

        else: 
            print("No valid connection engine available. Data transfer to MySQL database failed.") # print a message indicating that there is no valid connection engine available, which means that the data transfer to the MySQL database has failed.            
            return False # return False to indicate that the data transfer was not successful

    except Exception as e:
        print(f"Error transferring data to MySQL database: {e}") # print an error message if there is an issue during the data transfer process, including the exception message for debugging purposes. 

        return False # return False to indicate that there was an error during the data transfer process

    finally:
        if not engine is None:
            engine.dispose() # dispose of the database connection engine to free up resources


# In[19]:


def process_data_transfer_toSQLDB(df, contenttype, dbtype):

    try:
        # Declare/initialize variables at start

        conn_db=None # initialize a variable to hold the database connection, which will be used later to establish a connection to the PostgreSQL database and transfer data from Python to the database.


        if contenttype.lower() == 'customer_shopping_behavior': 

            if dbtype.lower() =='postgresql' or dbtype.lower() =='mysql':
                conn_db = DB_Config(dbtype) # establish a connection object to the PostgreSQL database using the connectDB_postgreSQL function defined earlier.           

            else:
                print(f"Database type '{dbtype}' is not recognized. No data transfer will be performed.") # print a message indicating that the provided database type is not recognized and that no data transfer will be performed.

        else:
            print(f"Content type '{contenttype}' is not recognized. No data transfer will be performed.") # print a message indicating that the provided content type is not recognized and that no data transfer will be performed.



        if not conn_db is None:          

            if dbtype.lower() =='postgresql':
                success= DBConnect_PostgreSQL(df, 'customer', conn_db) # transfer the cleaned and processed DataFrame to the PostgreSQL database using the transfer_data_to_postgres function defined earlier, specifying the table name as 'customer_shopping_behavior'.    

                if success:
                    print("Data transfer to PostgreSQL database successful.")
                else:
                    print("Data transfer to PostgreSQL database failed.")           

            elif dbtype.lower() =='mysql':          
                success= DBConnect_MySQL(df, 'customer', conn_db) # transfer the cleaned and processed DataFrame to the MySQL database using the transfer_data_to_mysql function defined earlier, specifying the table name as 'customer_shopping_behavior'.    

                if success:
                    print("Data transfer to MySQL database successful.")
                else:
                    print("Data transfer to MySQL database failed.")


        else:
            print("No valid connection string available. Data transfer to database failed.") # print a message indicating that there is no valid connection string available, which means that the data transfer to the database has failed.            


    except Exception as e:
        print(f"Error processing data transfer: {e}")        


# In[ ]:


process_data_transfer_toSQLDB(df, 'customer_shopping_behavior', 'PostgresSQL'); # call the function to transfer data from Python to PostgreSQL database, passing the cleaned and processed DataFrame and the target table name as arguments.')

#process_data_transfer_toSQLDB(df, 'customer_shopping_behavior', 'MySQL'); # call the function to transfer data from Python to MySQL database, passing the cleaned and processed DataFrame and the target table name as arguments.')

