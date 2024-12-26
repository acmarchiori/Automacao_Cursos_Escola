import os
import requests
import zipfile
import rarfile
import pandas as pd
from sqlalchemy.sql import text
import base64
import unicodedata
from bs4 import BeautifulSoup
import shutil

# Defina as variáveis de tipo de curso, tipo de aula, AREA_BNCC e cor no início
TIPO_CURSO = "ESCOLAR"
AREA_BNCC = "LP"
COR = "#EB7D36"
DOWNLOAD_PATH = "/home/acmarchiori/Área de Trabalho/HTLM_LITERATURA"


def normalizar_texto(texto):
    """Função para remover acentos e normalizar o texto."""
    return unicodedata.normalize('NFKD', texto) \
        .encode('ASCII', 'ignore') \
        .decode('utf-8') \
        .lower()


def baixar_arquivo_google_drive(url, destino):
    """Baixa um arquivo do Google Drive."""
    print(f"Iniciando download do Google Drive: {url}")  # Log de depuração
    if "drive.google.com" not in url:
        print(f"Erro: URL do Google Drive inválida: {url}")
        return False

    try:
        if '/d/' in url:
            file_id = url.split('/d/')[1].split('/')[0]
        elif 'id=' in url:
            file_id = url.split('id=')[1].split('&')[0]
        else:
            raise IndexError
    except IndexError:
        print(f"Erro: URL do Google Drive inválida: {url}")
        return False

    download_url = (
        f"https://drive.google.com/u/0/uc?id={file_id}&export=download"
    )
    print(f"Link de download convertido: {download_url}")  # Log de depuração

    headers = {
      "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
      )
    }

    try:
        response = requests.get(download_url, headers=headers, timeout=10)
        # Levanta um erro para códigos de status HTTP ruins
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar o arquivo: {e}")
        return False

    # Determinar o nome do arquivo a partir do cabeçalho de resposta
    content_disposition = response.headers.get('content-disposition')
    if content_disposition:
        filename = content_disposition.split('filename=')[1].strip('"')
    else:
        filename = f"{file_id}.zip"
        # Nome padrão caso não seja possível determinar

    # Garantir que o diretório de destino exista
    os.makedirs(os.path.dirname(destino), exist_ok=True)

    destino = os.path.join(os.path.dirname(destino), filename)
    with open(destino, 'wb') as f:
        f.write(response.content)
    print(f"Arquivo baixado com sucesso: {destino}")  # Log de depuração
    return destino


def extrair_arquivo(arquivo, destino):
    """Extrai um arquivo zip ou rar para o destino especificado e
    retorna o caminho do HTML e da pasta de imagens."""
    # Log de depuração
    print(f"Iniciando extração do arquivo: {arquivo}")
    try:
        if arquivo.lower().endswith('.zip'):
            with zipfile.ZipFile(arquivo, 'r') as zip_ref:
                zip_ref.extractall(destino)
        elif arquivo.lower().endswith('.rar'):
            with rarfile.RarFile(arquivo, 'r') as rar_ref:
                rar_ref.extractall(destino)
        else:
            print(
              f"Erro: O arquivo {arquivo} não é um arquivo ZIP ou RAR válido."
            )
            return None, None
        # Log de depuração
        print(f"Arquivo {arquivo} extraído para {destino}")
    except (zipfile.BadZipFile, rarfile.BadRarFile):
        print(f"Erro: O arquivo {arquivo} não é um arquivo ZIP ou RAR válido.")
        return None, None

    # Localizar o arquivo HTML e a pasta de imagens
    html_file = None
    pasta_imagens = None

    for root, dirs, files in os.walk(destino):
        for file in files:
            if file.lower().endswith(".htm") or file.lower().endswith(".html"):
                # Verifica se o arquivo não é um header ou similar
                if "header" not in file.lower():
                    html_file = os.path.join(root, file)
        for dir in dirs:
            if "arquivos" in dir.lower():
                pasta_imagens = os.path.join(root, dir)

    if html_file and pasta_imagens:
        # Log de depuração
        print(f"Arquivo HTML localizado: {html_file}")
        print(f"Pasta de imagens localizada: {pasta_imagens}")
    else:
        print(
            f"Erro: Arquivo HTML ou pasta de imagens não encontrados em "
            f"{destino}"
        )

    return html_file, pasta_imagens


def converter_imagens_para_base64(html_content, pasta_imagens):
    """Converte todas as imagens encontradas no HTML para base64 e substitui"""
    """o src das imagens por dados base64 no próprio HTML."""
    print("Iniciando conversão de imagens para base64...")  # Log de depuração
    soup = BeautifulSoup(html_content, 'html.parser')

    for img_tag in soup.find_all('img'):
        img_src = img_tag.get('src', '')

        if img_src.startswith('http'):
            continue  # Ignora imagens externas (URLs)

        img_file_name = os.path.basename(img_src)
        img_path = os.path.join(pasta_imagens, img_file_name)

        if os.path.exists(img_path):
            ext = img_file_name.split('.')[-1]
            with open(img_path, 'rb') as img_file:
                img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                # Log de depuração
                print(f"Convertendo imagem {img_file_name} para base64...")
                img_tag['src'] = f"data:image/{ext};base64,{img_base64}"
        else:
            # Log de depuração
            print(f"Imagem não encontrada: {img_file_name}")

    print("Conversão de imagens para base64 concluída.")  # Log de depuração
    return str(soup)


def processar_cursos(df):
    cursos = df[[
        "ANO_ESCOLAR", "SEGMENTO_ESCOLAR", "TITULO",
        "ORDEM_MODULO", "NOME_MODULO", "ORDEM_CAPITULO",
        "NOME_CAPITULO", "ORDEM_AULA", "TITULO_AULA",
        "PALAVRAS_CHAVES", "CODIGOS_BNCC", "NIVEL",
        "LINK_CONTEUDO", "ESTRATEGIA_APRENDIZAGEM",
        "TIPO_AULA",
    ]].drop_duplicates()

    cursos["COR"] = COR
    cursos["TIPO_CURSO"] = TIPO_CURSO
    cursos["AREA_BNCC"] = AREA_BNCC
    return cursos


def inserir_cursos(engine, cursos_df, nome_planilha):
    # Dicionários para rastrear IDs já inseridos
    modulos_inseridos = {}
    capitulos_inseridos = {}
    ultimo_fk_curso = None
    ultimo_fk_modulo = None
    ultimo_fk_capitulo = None
    with engine.begin() as conn:
        for idx, row in cursos_df.iterrows():
            print(f"Processando curso: {row['TITULO']}")  # Log de depuração
            # Buscar IDs necessários
            segmento_id = conn.execute(
                text(
                    "SELECT ID FROM T_SEGMENTOS_ESCOLARES WHERE NOME=:nome"
                ),
                {"nome": row["SEGMENTO_ESCOLAR"]}
            ).scalar()

            ano_id = conn.execute(
                text(
                    "SELECT ID FROM T_ANOS_ESCOLARES WHERE NOME=:nome "
                    "AND FK_SEGMENTO_ESCOLAR=:fk_segmento"
                ),
                {"nome": row["ANO_ESCOLAR"], "fk_segmento": segmento_id}
            ).scalar()

            area_bncc_id = conn.execute(
                text(
                    "SELECT ID FROM T_AREAS_BNCC WHERE CODIGO=:codigo"
                ),
                {"codigo": "LP"}
            ).scalar()

            if not segmento_id or not ano_id or not area_bncc_id:
                print(
                    f"Erro: Segmento, Ano escolar ou Área BNCC não encontrado "
                    f"para o curso {row['TITULO']}"
                )
                continue

            curso_id = conn.execute(
                text(
                    "SELECT ID FROM T_CURSOS WHERE TITULO=:titulo"
                ),
                {"titulo": row["TITULO"]}
            ).scalar()

            if not curso_id:
                result = conn.execute(
                    text(
                        "EXEC dbo.sp_inserir_curso "
                        "@titulo=:titulo, "
                        "@tipo_curso=:tipo_curso, "
                        "@cor=:cor, "
                        "@segmento_escolar=:segmento_escolar, "
                        "@ano_escolar=:ano_escolar, "
                        "@area_bncc=:area_bncc, "
                        "@novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "titulo": row["TITULO"],
                        "tipo_curso": TIPO_CURSO,
                        "cor": row["COR"],
                        "segmento_escolar": segmento_id,
                        "ano_escolar": ano_id,
                        "area_bncc": area_bncc_id,
                        "novo_id": 0,
                    },
                )
                curso_id = result.scalar()
                acao = "INSERT"
                # Log de depuração
                print(f"Curso '{row['TITULO']}' inserido com ID {curso_id}")
            else:
                acao = "SELECT"
                print(f"Curso '{row['TITULO']}' já existe com ID {curso_id}")

            # Inserir registro na tabela T_LOG_IMPORTACAO se o
            # FK_CURSO for diferente do último inserido
            if curso_id != ultimo_fk_curso:
                conn.execute(
                  text(
                    "INSERT INTO T_LOG_IMPORTACAO (NOME_PLANILHA, FK_CURSO, "
                    "FK_MODULO, FK_CAPITULO, FK_AULA, ACAO, TIMESTAMP, "
                    "FK_NIVEL, FK_SEGMENTO, FK_ANO) "
                    "VALUES (:nome_planilha, :fk_curso, NULL, NULL, NULL, "
                    ":acao, GETDATE(), NULL, :fk_segmento, :fk_ano)"
                  ),
                  {
                    "nome_planilha": nome_planilha,
                    "fk_curso": curso_id,
                    "acao": acao,
                    "fk_segmento": segmento_id,
                    "fk_ano": ano_id
                  }
                )
                # Atualizar o último FK_CURSO inserido
                ultimo_fk_curso = curso_id

            # Processar módulos
            modulo_chave = (row["NOME_MODULO"], row["ORDEM_MODULO"])
            modulo_id = modulos_inseridos.get(modulo_chave) or conn.execute(
                text(
                    "SELECT ID FROM T_MODULOS WHERE NOME=:nome "
                    "AND FK_CURSO=:fk_curso"
                ),
                {"nome": row["NOME_MODULO"], "fk_curso": curso_id}
            ).scalar()

            if not modulo_id:
                modulo_result = conn.execute(
                    text(
                        "EXEC dbo.sp_inserir_modulo "
                        "@nome=:nome, "
                        "@ordem=:ordem, "
                        "@fk_curso=:fk_curso, "
                        "@novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "nome": row["NOME_MODULO"],
                        "ordem": row["ORDEM_MODULO"],
                        "fk_curso": curso_id,
                        "novo_id": 0,
                    },
                )
                modulo_id = modulo_result.scalar()
                acao = "INSERT"
                # Log de depuração
                print(
                  f"Módulo '{row['NOME_MODULO']}' inserido com ID {modulo_id}"
                )
            else:
                acao = "SELECT"
                print(
                  f"Módulo '{row['NOME_MODULO']}' já existe com ID {modulo_id}"
                )

            # Inserir registro na tabela T_LOG_IMPORTACAO
            if modulo_id != ultimo_fk_modulo:
                conn.execute(
                  text(
                    "INSERT INTO T_LOG_IMPORTACAO (NOME_PLANILHA, FK_CURSO, "
                    "FK_MODULO, FK_CAPITULO, FK_AULA, ACAO, TIMESTAMP, "
                    "FK_NIVEL, FK_SEGMENTO, FK_ANO) "
                    "VALUES (:nome_planilha, :fk_curso, :fk_modulo, NULL, "
                    "NULL, :acao, GETDATE(), NULL, :fk_segmento, :fk_ano)"
                  ),
                  {
                    "nome_planilha": nome_planilha,
                    "fk_curso": curso_id,
                    "fk_modulo": modulo_id,
                    "acao": acao,
                    "fk_segmento": segmento_id,
                    "fk_ano": ano_id
                  }
                )
                # Atualizar o último FK_MODULO inserido
                ultimo_fk_modulo = modulo_id

            modulos_inseridos[modulo_chave] = modulo_id

            # Processar capítulos
            capitulo_chave = (
              modulo_id, row["NOME_CAPITULO"], row["ORDEM_CAPITULO"])
            capitulo_id = capitulos_inseridos.get(capitulo_chave)

            if not capitulo_id:
                capitulo_result = conn.execute(
                    text(
                        "EXEC dbo.sp_inserir_capitulo "
                        "@nome=:nome, "
                        "@ordem=:ordem, "
                        "@fk_modulo=:fk_modulo, "
                        "@novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "nome": row["NOME_CAPITULO"],
                        "ordem": int(row["ORDEM_CAPITULO"]),
                        "fk_modulo": modulo_id,
                        "novo_id": 0,
                    },
                )
                capitulo_id = capitulo_result.scalar()
                acao = "INSERT"
                # Log de depuração
                print(
                  f"Capítulo '{row['NOME_CAPITULO']}' inserido com ID "
                  f"{capitulo_id}"
                )
            else:
                acao = "SELECT"
                print(
                  f"Capítulo '{row['NOME_CAPITULO']}' já existe com ID "
                  f"{capitulo_id}"
                )

            # Inserir registro na tabela T_LOG_IMPORTACAO
            if capitulo_id != ultimo_fk_capitulo:
                conn.execute(
                  text(
                    "INSERT INTO T_LOG_IMPORTACAO (NOME_PLANILHA, FK_CURSO, "
                    "FK_MODULO, FK_CAPITULO, FK_AULA, ACAO, TIMESTAMP, "
                    "FK_NIVEL, FK_SEGMENTO, FK_ANO) "
                    "VALUES (:nome_planilha, :fk_curso, :fk_modulo, "
                    ":fk_capitulo, NULL, :acao, GETDATE(), NULL, "
                    ":fk_segmento, :fk_ano)"
                  ),
                  {
                    "nome_planilha": nome_planilha,
                    "fk_curso": curso_id,
                    "fk_modulo": modulo_id,
                    "fk_capitulo": capitulo_id,
                    "acao": acao,
                    "fk_segmento": segmento_id,
                    "fk_ano": ano_id
                  }
                )
                # Atualizar o último FK_CAPITULO inserido
                ultimo_fk_capitulo = capitulo_id

            capitulos_inseridos[capitulo_chave] = capitulo_id

            # Processar tipo de aula
            tipo_aula_nome_normalizado = normalizar_texto(row["TIPO_AULA"])
            tipo_aula_id = conn.execute(
                text(
                    "SELECT ID FROM T_TIPO_AULA "
                    "WHERE LOWER(REPLACE"
                    "(REPLACE(REPLACE"
                    "(NOME, 'ã', 'a'), 'é', 'e'), 'ç', 'c')) = :nome"
                ),
                {"nome": tipo_aula_nome_normalizado}
            ).scalar()

            if not tipo_aula_id:
                print(f"Erro: Tipo de Aula '{row['TIPO_AULA']}' "
                      f"não encontrado!")
                continue

            nivel = row['NIVEL']
            nivel_complexidade_id = None
            if nivel:
                if isinstance(nivel, str) and nivel.startswith("Nível"):
                    nivel = nivel.split()[-1]
                nivel = f"Nível {int(nivel)}"
                nivel_complexidade_id = conn.execute(
                    text(
                        "SELECT ID FROM T_NIVEL_COMPLEXIDADE "
                        "WHERE DESCRICAO=:descricao"
                    ),
                    {"descricao": nivel}
                ).scalar()

            if not nivel_complexidade_id and nivel is not None:
                print(f"Erro: Nível de Complexidade '{nivel}' não encontrado!")
                continue

            estrategia_id = conn.execute(
                text(
                    "SELECT ID FROM T_ESTRATEGIA_APRENDIZAGEM WHERE "
                    "UPPER(DESCRICAO) = UPPER(:descricao)"
                ),
                {"descricao": row["ESTRATEGIA_APRENDIZAGEM"]}
            ).scalar()

            if not estrategia_id:
                print(
                  f"Erro: Estratégia de Aprendizagem '"
                  f"{row['ESTRATEGIA_APRENDIZAGEM']}' não encontrada!"
                  )
                continue

            # Certifique-se de que 'ORDEM_AULA' seja um inteiro
            ordem_aula = int(row[
                "ORDEM_AULA"]) if row["ORDEM_AULA"] is not None else None

            aula_id = conn.execute(
                text(
                    "SELECT ID FROM T_AULAS WHERE TITULO=:titulo AND "
                    "FK_CAPITULO=:fk_capitulo AND ORDEM=:ordem"
                ),
                {
                    "titulo": row["TITULO_AULA"],
                    "fk_capitulo": capitulo_id,
                    "ordem": ordem_aula
                }
            ).scalar()

            conteudo_aula = None
            if not aula_id:
                # Verificar se há um link para o Google Drive
                if not pd.isna(row["LINK_CONTEUDO"]) and \
                  row["LINK_CONTEUDO"].strip():
                    temp_dir = os.path.join(DOWNLOAD_PATH, f"aula_{idx}")
                    os.makedirs(temp_dir, exist_ok=True)
                    # Determinar a extensão do arquivo a partir do link
                    arquivo_extensao = row["LINK_CONTEUDO"].split('.')[-1]
                    arquivo_path = os.path.join(
                        temp_dir, f"conteudo.{arquivo_extensao}")
                    # Log de depuração
                    print(
                      f"Baixando conteúdo para a aula "
                      f"'{row['TITULO_AULA']}'..."
                      )
                    arquivo_baixado = baixar_arquivo_google_drive(
                      row["LINK_CONTEUDO"], arquivo_path
                    )
                    if arquivo_baixado:
                        # Log de depuração
                        print(
                          f"Extraindo conteúdo para a aula '"
                          f"{row['TITULO_AULA']}'..."
                          )
                        html_file, pasta_imagens = extrair_arquivo(
                          arquivo_baixado, temp_dir
                        )

                        if html_file and pasta_imagens:
                            # Alterado para 'latin-1'
                            with open(html_file, 'r', encoding='latin-1') as f:
                                conteudo_html = f.read()
                            print(
                              f"Convertendo imagens para a aula '"
                              f"{row['TITULO_AULA']}'..."
                              )  # Log de depuração
                            conteudo_aula = converter_imagens_para_base64(
                              conteudo_html, pasta_imagens
                            )
                            print(
                              f"Conteúdo HTML convertido para a aula '"
                              f"{row['TITULO_AULA']}': "
                              f"{conteudo_aula[:500]}..."
                              )  # Log de depuração
                        else:
                            print(
                              f"Erro: Arquivo HTML ou pasta "
                              f"de imagens não encontrados em {temp_dir}"
                              )

                        # Remover o diretório temporário após processamento
                        shutil.rmtree(temp_dir)

                aula_result = conn.execute(
                    text(
                        "EXEC sp_inserir_aula "
                        "@titulo=:titulo, "
                        "@ordem=:ordem, "
                        "@fk_capitulo=:fk_capitulo, "
                        "@fk_curso=:fk_curso, "
                        "@fk_modulo=:fk_modulo, "
                        "@fk_tipo_aula=:fk_tipo_aula, "
                        "@palavras_chaves=:palavras_chaves, "
                        "@fk_nivel_complexidade=:fk_nivel_complexidade, "
                        "@conteudo_aula=:conteudo_aula, "
                        "@fk_estrategia_aprendizagem=:"
                        "fk_estrategia_aprendizagem, "
                        "@novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "titulo": row["TITULO_AULA"],
                        "ordem": row["ORDEM_AULA"],
                        "palavras_chaves": row["PALAVRAS_CHAVES"],
                        "fk_capitulo": capitulo_id,
                        "fk_curso": curso_id,
                        "fk_modulo": modulo_id,
                        "fk_tipo_aula": tipo_aula_id,
                        "fk_nivel_complexidade": nivel_complexidade_id,
                        "conteudo_aula": conteudo_aula,
                        "fk_estrategia_aprendizagem": estrategia_id,
                        "novo_id": 0,
                    },
                )
                aula_id = aula_result.scalar()
                acao = "INSERT"
                # Log de depuração
                print(f"Aula '{row['TITULO_AULA']}' inserida com ID {aula_id}")
            else:
                acao = "SELECT"
                print(
                  f"Aula '{row['TITULO_AULA']}' já existe com ID {aula_id}"
                )

            # Inserir registro na tabela T_LOG_IMPORTACAO
            conn.execute(
              text(
                "INSERT INTO T_LOG_IMPORTACAO (NOME_PLANILHA, FK_CURSO, "
                "FK_MODULO, FK_CAPITULO, FK_AULA, ACAO, TIMESTAMP, "
                "FK_NIVEL, FK_SEGMENTO, FK_ANO) "
                "VALUES (:nome_planilha, :fk_curso, :fk_modulo, "
                ":fk_capitulo, :fk_aula, :acao, GETDATE(), :fk_nivel, "
                "NULL, NULL)"
              ),
              {
                "nome_planilha": nome_planilha,
                "fk_curso": curso_id,
                "fk_modulo": modulo_id,
                "fk_capitulo": capitulo_id,
                "fk_aula": aula_id,
                "acao": acao,
                "fk_nivel": nivel_complexidade_id
              }
            )

            # Processar códigos BNCC
            codigos_bncc = []
            if row["CODIGOS_BNCC"]:
                codigos_bncc = row["CODIGOS_BNCC"].split(';')
            bncc_habilidades = (
              ";".join([codigo.strip() for codigo in codigos_bncc])
              if codigos_bncc else None
            )

            try:
                conn.execute(
                  text(
                    "EXEC sp_inserir_bncc_aula "
                    ":fk_aula, :bncc_habilidades, :linha"
                  ),
                  {
                    "fk_aula": aula_id,
                    "bncc_habilidades": bncc_habilidades,
                    "linha": idx + 1
                  }
                )
                print(
                  f"Códigos BNCC inseridos para a aula ID {aula_id} "
                  f"na linha {idx + 1}"
                )  # Log de depuração
            except Exception as e:
                print(f"Erro ao inserir códigos BNCC na linha {idx + 1}: {e}")

        # Chamar a procedure para remover comentários HTML após a inserção
        # das aulas
        try:
            conn.execute(text("EXEC dbo.RemoveAllHtmlComments"))
            print("Comentários HTML removidos com sucesso.")
        except Exception as e:
            print(f"Erro ao remover comentários HTML: {e}")
