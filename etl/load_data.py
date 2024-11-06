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
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?"
        f"driver={driver}&Encrypt=yes&TrustServerCertificate=yes&Connection "
        f"Timeout=30"
    )
    return create_engine(connection_string)


def carregar_dados():
    # Carregar a planilha
    file_path = (
        "/home/acmarchiori/Área de Trabalho/"
        "2024_BANCO DE MATERIAIS_LITERATURA.xlsm"
    )

    # Atualize com o nome correto da aba
    df = pd.read_excel(file_path, sheet_name="Plan1")

    # Imprimir as colunas do DataFrame
    print("Colunas disponíveis no DataFrame:", df.columns.tolist())

    # Renomear colunas conforme necessário
    df.rename(
        columns={
            "ANO": "ANO_ESCOLAR",
            "SEGMENTO": "SEGMENTO_ESCOLAR",
            "ÁREA": "TITULO",
            "FRENTE": "NOME_MODULO",
            "ORDEM MÓDULO": "ORDEM_MODULO",
            "TEMA": "NOME_CAPITULO",
            "ORDEM CAPITULO": "ORDEM_CAPITULO",
            "CONTEÚDO": "TITULO_AULA",
            "ORDEM AULA": "ORDEM_AULA",
            "BNCC": "CODIGOS_BNCC",
            "PALAVRAS CHAVES": "PALAVRAS_CHAVES",
        },
        inplace=True,
    )

    # Garantir que valores nulos ou vazios em "PALAVRAS_CHAVES" sejam
    # convertidos para None
    df['PALAVRAS_CHAVES'] = df['PALAVRAS_CHAVES'].apply(
        lambda x: None if pd.isna(x) or x == '' else x
    )

    # Processar os dados
    cursos_df = processar_cursos(df)

    # Criar engine de conexão
    engine = criar_engine()

    # Chamar a função para inserir os cursos
    inserir_cursos(engine, cursos_df)
    print("Processamento completo. Todas as linhas foram processadas.")


if __name__ == "__main__":
    carregar_dados()
