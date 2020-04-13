import os
import sqlalchemy
import argparse
import pandas as pd
import sqlite3
import datetime
from olistlib.db import utils

# Endereços do nosso projeto e subpastas
DATA_PREP_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.abspath( DATA_PREP_DIR ))
DATA_DIR = os.path.join( BASE_DIR, 'data')

# Parser de data para fazer a foto
parser = argparse.ArgumentParser()
parser.add_argument('--date_end', '-e', help = 'Data de fim da extração', default = '2018-06-01')
args = parser.parse_args()

date_end = args.date_end

ano = int(date_end.split('-')[0]) - 1
mes = int(date_end.split('-')[1])

date_init = f'{ano}-{mes}-01'

# Importa a Query
query = utils.import_query(os.path.join(DATA_PREP_DIR, 'sgmt.sql'))

query = query.format(date_init = date_init,
                     date_end = date_end)

#Abrindo conexão com o banco usando lib SQLite3
conn = utils.connect_db()

# #Abrindo conexão com o banco usando lib SqlAlchemy
conn_alchemy = utils.connect_db_alchemy()

create_query = f'''
CREATE TABLE tb_seller_sgmt AS 
{query}
;'''

insert_query = f'''
DELETE FROM tb_seller_sgmt WHERE DT_SGMT = '{date_end}';
INSERT INTO tb_seller_sgmt 
{query};'''

try:
    conn_alchemy.execute( create_query )
except:
    conn.executescript( insert_query )