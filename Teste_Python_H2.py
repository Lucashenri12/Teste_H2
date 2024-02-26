Teste_Python_H2

import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

# Configurações de conexão para o banco de dados remoto
conf_remoto = {
     'host': '40b8f30251.nxcli.io',
     'user': 'a4f2b49a_padawan',
     'password': 'KaratFlanksUgliedSpinal',
     'database': 'a4f2b49a_sample_database'
}

# Configurações de conexão para o banco de dados local
conf_local = {
   'host': 'localhost',
   'user': 'lucas',
   'password': 'Lucas@2506',
   'database': 'DB_LOCAL',
   'port': '3306'
}

# Nome da tabela remota e local
tabela_remota = 'raw_data'
tabela_local = 'trusted_data'

# Cria uma conexão com o banco de dados remoto usando sqlalchemy
engine_remoto = create_engine(f'mysql+mysqlconnector://{conf_remoto["user"]}:{conf_remoto["password"]}@{conf_remoto["host"]}/{conf_remoto["database"]}')

# Lê os dados da tabela remota usando pandas
df = pd.read_sql_table(tabela_remota, engine_remoto)

# Converter a coluna 'datahora_acesso' para formato de data
df['datahora_acesso'] = pd.to_datetime(df['datahora_acesso'], errors='coerce')

# Extrair o mês da coluna 'datahora_acesso'
df['mes'] = df['datahora_acesso'].dt.to_period('M')

# Consolidar os dados agregando por mês
df_consolidado = df.groupby('mes').agg(
    rake_total=('rake', 'sum'),
    jogadores=('clientes_id', 'nunique'),
    rake_cash_game=('rake', lambda x: x[df['modalidade'] == 'Cash Game'].sum()),
    rake_torneio=('rake', lambda x: x[df['modalidade'] == 'Torneio'].sum()),
    jogadores_cash_game=('clientes_id', lambda x: x[df['modalidade'] == 'Cash Game'].nunique()),
    jogadores_torneio=('clientes_id', lambda x: x[df['modalidade'] == 'Torneio'].nunique())
)

# Criar uma conexão manualmente usando mysql.connector
connection = mysql.connector.connect(
    host=conf_local['host'],
    user=conf_local['user'],
    password=conf_local['password'],
    database=conf_local['database'],
    port=conf_local['port']
)

# Passar a conexão para a engine do SQLAlchemy
engine_local = create_engine('mysql+mysqlconnector://', creator=lambda: connection)

# Salva o DataFrame no banco de dados local
df_consolidado.to_sql(name=tabela_local, con=engine_local, if_exists='replace', index=False)

print("Dados consolidados e salvos com sucesso!")

