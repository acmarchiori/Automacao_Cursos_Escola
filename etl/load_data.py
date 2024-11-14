import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from utils import processar_cursos, inserir_cursos

load_dotenv()


# Configuração de conexão com o banco de dados SQL Server
def criar_engine():
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER")
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
        "2024_BANCO DE MATERIAIS_LITERATURA_1.xlsm"
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
            "MÓDULO": "NOME_MODULO",
            "ORDEM MÓDULO": "ORDEM_MODULO",
            "CAPÍTULO": "NOME_CAPITULO",
            "ORDEM CAPÍTULO": "ORDEM_CAPITULO",
            "AULA": "TITULO_AULA",
            "ORDEM AULA": "ORDEM_AULA",
            "BNCC": "CODIGOS_BNCC",
            "PALAVRAS CHAVES": "PALAVRAS_CHAVES",
            "NÍVEL": "NIVEL",
            "LINK/CONTEÚDO": "LINK_CONTEUDO",
            "ESTRATÉGIA DE APRENDIZAGEM": "ESTRATEGIA_APRENDIZAGEM",
            "TIPO DE AULA": "TIPO_AULA",
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
