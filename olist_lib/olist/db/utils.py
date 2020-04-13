import pandas as pd
import os
import sqlite3
import sqlalchemy
from tqdm import tqdm
import dotenv

BASE_DIR = os.path.dirname( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) )
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'olist.db')

def import_query(path, **kwargs):
    '''
    Essa função realiza o import de uma query, onde pode ser passado vários argumentos de import (read())
    '''
    with open(path, 'r', **kwargs) as query_file:
        query = query_file.read()

    return query

def connect_db(db_name, dotenv_path = os.path.expanduser(r'C:\Users\yago1\Desktop\olist\upload_data\src\.env')):
    '''
    Função para conectar ao banco de dados (sqlite3)
    '''
    dotenv.load_dotenv( dotenv_path )

    host = os.getenv('HOST_' + db_name.upper())
    port = os.getenv('PORT_' + db_name.upper())
    user = os.getenv('USUARIO_' + db_name.upper())
    pswd = os.getenv('PASS_' + db_name.upper())
    
    if (db_name == 'mysql'):
        str_connection = f'mysql+pymysql://{user}:{pswd}@{host}:{port}'
        return (sqlalchemy.create_engine(str_connection))
  
    else:
        str_connection = DB_PATH
        connection = sqlite3.connect(str_connection).cursor()
        return (connection)

def connect_db_alchemy():
    '''
    Função para conectar ao banco de dados (sqlite3) usando SqlAlchemy
    '''
    str_connection = 'sqlite:///{path}'
    str_connection = str_connection.format(path = os.path.join(DATA_DIR, 'olist.db'))
    connection = sqlalchemy.create_engine( str_connection )
    return (connection)

def execute_many_sql(sql, conn):
    conn.executescript(sql)