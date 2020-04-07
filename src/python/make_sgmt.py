import os
import sqlalchemy
import argparse
import pandas as pd
import sqlite3
import datetime

# Endereços do nosso projeto e subpastas
BASE_DIR = os.path.dirname(os.path.abspath('data'))
DATA_DIR = os.path.join( BASE_DIR, 'data')
SQL_DIR = os.path.join(BASE_DIR, 'src', 'sql')

# Parser de data para fazer a foto
parser = argparse.ArgumentParser()
parser.add_argument('--date_end', '-e', help = 'Data de fim da extração', default = '2018-06-01')
args = parser.parse_args()

date_end = args.date_end

ano = int(date_end.split('-')[0]) - 1
mes = int(date_end.split('-')[1])

date_init = f'{ano}-{mes}-01'

# Importa a Query
with open( os.path.join(SQL_DIR, 'sgmt.sql')) as query_file:
    query = query_file.read()

query = query.format(date_init = date_init,
                     date_end = date_end)

#Abrindo conexão com o banco usando lib SQLite3
str_connection = os.path.join(DATA_DIR, 'olist.db')
connection = sqlite3.connect(str_connection).cursor()

# #Abrindo conexão com o banco usando lib SqlAlchemy
# str_connection = 'sqlite:///{path}'
# str_connection = str_connection.format(path = os.path.join(DATA_DIR, 'olist.db'))
# conexao = sqlalchemy.create_engine( str_connection,  pool_pre_ping = True )

create_query = f'''
CREATE TABLE tb_seller_sgmt AS 
{query}
;'''

insert_query = f'''
DELETE FROM tb_seller_sgmt WHERE DT_SGMT = '{date_end}';
INSERT INTO tb_seller_sgmt 
{query};'''

try:
    conexao.execute( create_query )
except:
    connection.executescript( insert_query )