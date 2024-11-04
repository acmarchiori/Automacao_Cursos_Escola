import pandas as pd
from sqlalchemy import create_engine
from utils import processar_cursos, inserir_cursos

# Configuração de conexão com o banco de dados SQL Server
def criar_engine():
    server = "18.224.230.12"
    database = "EPM_MOSAICO"
    username = "usr_mosaico"
    password = "fNntAxwEMTTrjV4K"
    driver = "ODBC Driver 17 for SQL Server"
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}&Encrypt=yes&TrustServerCertificate=yes&Connection Timeout=30"
    return create_engine(connection_string)

def carregar_dados():
    # Carregar a planilha
    file_path = "/home/acmarchiori/Área de Trabalho/2024_BANCO DE MATERIAIS_LITERATURA.xlsm"  # Atualize com o caminho correto
    # Atualize com o nome correto da aba
    df = pd.read_excel(file_path, sheet_name="Plan1")

    # Imprimir as colunas do DataFrame
    print("Colunas disponíveis no DataFrame:", df.columns.tolist())  # Verifica as colunas

    # Processar os dados
    cursos_df = processar_cursos(df)

    # Criar engine de conexão
    engine = criar_engine()

    # Chamar a função para inserir os cursos
    inserir_cursos(engine, cursos_df)

if __name__ == "__main__":
    carregar_dados()
