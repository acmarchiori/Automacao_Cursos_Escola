import os
from sqlalchemy import create_engine, text


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


def inserir_conteudo_aula(engine, id_aula, conteudo):
    with engine.begin() as conn:
        query = text(
            "UPDATE T_AULAS SET CONTEUDO_AULA = :conteudo WHERE ID = :id_aula")
        conn.execute(query, {"conteudo": conteudo, "id_aula": id_aula})
    print(f"Conteúdo da aula com ID {id_aula} atualizado com sucesso.")


def carregar_conteudo_html(caminho_arquivo):
    with open(caminho_arquivo, 'r', encoding='utf-8') as file:
        conteudo = file.read()
    return conteudo


def main():
    engine = criar_engine()
    id_aula = 376  # ID da aula que deseja atualizar
    caminho_arquivo_html = "/home/acmarchiori/Área de Trabalho/"
    "HTLM_LITERATURA/PORT_EM1_LIT_TROVADORISMO_CONT_N2_V1/"
    "PORT_EM1_LIT_TROVADORISMO_CONT_N2_V1.html"  # Caminho para o arquivo HTML

    if os.path.exists(caminho_arquivo_html):
        conteudo_html = carregar_conteudo_html(caminho_arquivo_html)
        inserir_conteudo_aula(engine, id_aula, conteudo_html)
    else:
        print(f"O arquivo {caminho_arquivo_html} não foi encontrado.")


if __name__ == "__main__":
    main()
