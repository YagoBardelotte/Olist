import os
import pandas as pd
from olistlib.db import utils

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data')
SQL_DIR = os.path.join( BASE_DIR, 'src', 'sql' )
DB_PATH = os.path.join( DATA_DIR, 'olist.db')

# Abre a conexão com o banco
conn = utils.connect_db_alchemy()

# Importa a query
query = utils.import_query( os.path.join( SQL_DIR, 'lifetime.sql' ) )

# Recebe, executa a query e envia para arquivo .csv
df = pd.read_sql_query( query, conn )
df.to_csv( os.path.join(DATA_DIR, 'lifetime.csv'), sep = ',', index = False )