import pandas as pd
import sqlite3
#CreatedBy Hamini TS


# Connecting to SQLlite DB
def get_db_connection():
    try:
        con = sqlite3.connect('assignment.db')
        c = con.cursor()
        c.execute('''DROP TABLE IF EXISTS logfile''')
        print("Connected to DB")
        return con
    except Exception as e:
        print(e)


# Loading the organized data to DB
def load_data_to_db(engine):
    try:
        #Specify the log20170201.csv file ptah or place it the same folder
        data = pd.read_csv('log20170201.csv')
        df = data.drop_duplicates()
        print("loading csv file to DB")
        df.to_sql(name='logfile', con=engine, if_exists='append', index=False)
        return True
    except Exception as e:
        print(e)


# Calculating the document size for each 30mins and retrieving top 10
def get_total_doc_size(df):
    try:
        grouped_df = df.groupby(pd.Grouper(key='time', freq='30min'))['size'].sum()
        result = pd.DataFrame({'time': grouped_df.index, 'size': grouped_df.values})
        result['time'] = result['time'].dt.strftime("%H:%M:%S")
        df_size = result.sort_values(by=['size'], ascending=False)[0:10]
        return df_size
    except Exception as e:
        print(e)


# Calculating the total count of the document for each 30mins session and retrieving top 10
def get_total_doc_count(df):
    try:
        grouped_df = df.groupby(pd.Grouper(key='time', freq='30min'))['extention'].count()
        result = pd.DataFrame({'time': grouped_df.index, 'extention': grouped_df.values})
        result['time'] = result['time'].dt.strftime("%H:%M:%S")
        df_doc_num = result.sort_values(by=['extention'], ascending=False)[0:10]
        return df_doc_num
    except Exception as e:
        print(e)


if __name__ == "__main__":
    try:
        # connectinf DB
        con = get_db_connection()
        # loading Data
        load_data_to_db(con)
        # Retrieving the loaded record for further implementation
        query = "Select * from logfile;"
        loaded_df = pd.read_sql(query, con)
        loaded_df['time'] = (pd.to_datetime(loaded_df['time'].str.strip(), format='%H:%M:%S'))
        size = get_total_doc_size(loaded_df)
        count = get_total_doc_count(loaded_df)
        size.to_csv('total_size.csv')
        count.to_csv('total_count.csv')
        print("Created csv file for total size and count")
    except Exception as e:
        print(e)
