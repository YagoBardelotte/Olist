import os
import pandas as pd
import sqlalchemy

# # Conexão com banco de dados na cloud
# usuario = 'root' # login
# senha = '' # senha
# endereco = 'localhost' #ip/host/dns
# porta = '3306' # porta

# str_connection = f'mysql+pymysql:///{usuario}:{senha}@{endereco}:{porta}'

# str_connection = str_connection.format(user = usuario, psw = senha, host = endereco, port = porta)
# conexao = sqlalchemy.create_engine( str_connection,  pool_pre_ping = True )

# BASE_DIR = os.path.dirname(os.path.abspath('data'))
# DATA_DIR = os.path.join( BASE_DIR, 'data')

# file_names = [file for file in os.listdir( DATA_DIR ) if file.endswith('.csv')]

# def data_quality(x):
#     if type(x) == str:
#         return x.replace('\n','').replace('\r','')
#     else:
#         return x

# for i in file_names:
#     df_tmp = pd.read_csv( os.path.join(DATA_DIR, i)).fillna('')
#     for c in df_tmp.columns:
#         df_tmp[c] = df_tmp[c].apply(data_quality)
#     table_name = 'tb_' + i.strip('.csv').replace('olist_', '').replace('_dataset', '')
#     df_tmp.to_sql( table_name, 
#                    conexao,
#                    schema = 'test',
#                    if_exists = 'replace',
#                    index = False )
                   
# # Não está funcionando retorna erro de acesso negado ao servidor


# Endereços do nosso projeto e subpastas
BASE_DIR = os.path.dirname(os.path.abspath('data'))
DATA_DIR = os.path.join( BASE_DIR, 'data')

#Encontrando os arquivos de dados
file_names = [file for file in os.listdir( DATA_DIR ) if file.endswith('.csv')]

# #Abrindo conexão com o banco
str_connection = 'sqlite:///{path}'
str_connection = str_connection.format(path = os.path.join(DATA_DIR, 'olist.db'))
conexao = sqlalchemy.create_engine( str_connection,  pool_pre_ping = True )

def data_quality(x):
    if type(x) == str:
        return x.replace('\n','').replace('\r','')
    else:
        return x


#Para cada arquivo é realizada uma inserção no banco
for i in file_names:
    df_tmp = pd.read_csv( os.path.join(DATA_DIR, i))
    for c in df_tmp.columns:
        df_tmp[c] = df_tmp[c].apply(data_quality)

    table_name = 'tb_' + i.strip('.csv').replace('olist_', '').replace('_dataset', '')
    df_tmp.to_sql( table_name, conexao )