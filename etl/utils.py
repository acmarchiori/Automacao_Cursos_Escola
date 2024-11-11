import os
from sqlalchemy.sql import text
import unicodedata

# Defina as variáveis de tipo de curso, tipo de aula, AREA_BNCC e cor no início
TIPO_CURSO = "ESCOLAR"
AREA_BNCC = "LP"
COR = "#EB7D36"
HTML_PATH = "/home/acmarchiori/Área de Trabalho/HTLM_LITERATURA"


def normalizar_texto(texto):
    """Função para remover acentos e normalizar o texto."""
    return unicodedata.normalize('NFKD', texto) \
        .encode('ASCII', 'ignore') \
        .decode('utf-8') \
        .lower()


def ler_html(nome_arquivo):
    """Função para ler o conteúdo de um arquivo HTML"""
    try:
        with open(
            os.path.join(HTML_PATH, nome_arquivo),
            'r',
            encoding='utf-8'
        ) as f:
            return f.read()
    except FileNotFoundError:
        print(f"Arquivo HTML {nome_arquivo} não encontrado.")
        return None


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


def inserir_cursos(engine, cursos_df):
    # Dicionários para rastrear IDs já inseridos
    modulos_inseridos = {}
    capitulos_inseridos = {}
    with engine.begin() as conn:
        for idx, row in cursos_df.iterrows():
            # Buscar IDs necessários
            segmento_id = conn.execute(
                text(
                    "SELECT ID FROM T_SEGMENTOS_ESCOLARES WITH(NOLOCK) "
                    "WHERE NOME=:nome"
                ),
                {"nome": row["SEGMENTO_ESCOLAR"]}
            ).scalar()

            ano_id = conn.execute(
                text(
                    "SELECT ID FROM T_ANOS_ESCOLARES WITH(NOLOCK) "
                    "WHERE NOME=:nome AND "
                    "FK_SEGMENTO_ESCOLAR=:fk_segmento"
                ),
                {"nome": row["ANO_ESCOLAR"], "fk_segmento": segmento_id}
            ).scalar()

            area_bncc_id = conn.execute(
                text(
                    "SELECT ID FROM T_AREAS_BNCC WITH(NOLOCK) "
                    "WHERE CODIGO=:codigo"
                ),
                {"codigo": "LP"}
            ).scalar()

            if not segmento_id or not ano_id or not area_bncc_id:
                print(f"Erro: Segmento, Ano escolar ou Área BNCC "
                      f"não encontrado para o curso {row['TITULO']}")
                continue

            curso_id = conn.execute(
                text(
                    "SELECT ID FROM T_CURSOS WITH(NOLOCK) WHERE TITULO=:titulo"
                ),
                {"titulo": row["TITULO"]}
            ).scalar()

            if not curso_id:
                result = conn.execute(
                    text(
                        "EXEC sp_inserir_curso @titulo=:titulo,"
                        " @tipo_curso=:tipo_curso,"
                        " @cor=:cor, @segmento_escolar=:segmento_escolar,"
                        " @ano_escolar=:ano_escolar,"
                        " @area_bncc=:area_bncc, @novo_id=:novo_id OUTPUT"
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
                print(f"Curso '{row['TITULO']}' inserido com ID {curso_id}")
            else:
                print(f"Curso '{row['TITULO']}' já existe com ID {curso_id}")

            # Processar módulos
            modulo_chave = (row["NOME_MODULO"], row["ORDEM_MODULO"])
            modulo_id = modulos_inseridos.get(modulo_chave) or conn.execute(
                text(
                    "SELECT ID FROM T_MODULOS WITH(NOLOCK) "
                    "WHERE NOME=:nome AND FK_CURSO=:fk_curso"
                ),
                {"nome": row["NOME_MODULO"], "fk_curso": curso_id}
            ).scalar()

            if not modulo_id:
                modulo_result = conn.execute(
                    text(
                        "EXEC sp_inserir_modulo @nome=:nome, @ordem=:ordem,"
                        " @fk_curso=:fk_curso, @novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "nome": row["NOME_MODULO"],
                        "ordem": row["ORDEM_MODULO"],
                        "fk_curso": curso_id,
                        "novo_id": 0,
                    },
                )
                modulo_id = modulo_result.scalar()
                print(f"Módulo '{row['NOME_MODULO']}' "
                      f"inserido com ID {modulo_id}")
            else:
                print(f"Módulo '{row['NOME_MODULO']}' "
                      f"já existe com ID {modulo_id}")

            modulos_inseridos[modulo_chave] = modulo_id

            # Processar capítulos
            capitulo_chave = (
                modulo_id,
                row["NOME_CAPITULO"],
                row["ORDEM_CAPITULO"]
            )
            capitulo_id = capitulos_inseridos.get(capitulo_chave) or \
                conn.execute(
                    text(
                        "SELECT ID FROM T_CAPITULOS WITH(NOLOCK) "
                        "WHERE NOME=:nome AND FK_MODULO=:fk_modulo"
                    ),
                    {"nome": row["NOME_CAPITULO"], "fk_modulo": modulo_id}
                ).scalar()

            if not capitulo_id:
                capitulo_result = conn.execute(
                    text(
                        "EXEC sp_inserir_capitulo @nome=:nome, @ordem=:ordem,"
                        " @fk_modulo=:fk_modulo, @novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "nome": row["NOME_CAPITULO"],
                        "ordem": row["ORDEM_CAPITULO"],
                        "fk_modulo": modulo_id,
                        "novo_id": 0,
                    },
                )
                capitulo_id = capitulo_result.scalar()
                print(f"Capítulo '{row['NOME_CAPITULO']}' "
                      f"inserido com ID {capitulo_id}")
            else:
                print(f"Capítulo '{row['NOME_CAPITULO']}' "
                      f"já existe com ID {capitulo_id}")

            capitulos_inseridos[capitulo_chave] = capitulo_id

            # Processar tipo de aula (normalizando o texto)
            tipo_aula_nome_normalizado = normalizar_texto(row["TIPO_AULA"])
            tipo_aula_id = conn.execute(
              text(
                "SELECT ID FROM T_TIPO_AULA WITH(NOLOCK) "
                "WHERE LOWER(REPLACE(REPLACE(REPLACE(NOME, 'ã', 'a'), "
                "'é', 'e'), 'ç', 'c')) = :nome"
              ),
              {"nome": tipo_aula_nome_normalizado}
            ).scalar()

            if not tipo_aula_id:
                print(
                    f"Erro: Tipo de Aula '{row['TIPO_AULA']}' não encontrado!"
                    )
                continue

            # Obter o ID do FK_NIVEL_COMPLEXIDADE com base no nível fornecido
            nivel = f"Nível {int(row['NIVEL'])}"
            nivel_complexidade_id = conn.execute(
                text(
                    "SELECT ID FROM T_NIVEL_COMPLEXIDADE WITH(NOLOCK) "
                    "WHERE DESCRICAO=:descricao"
                ),
                {"descricao": nivel}
            ).scalar()

            if not nivel_complexidade_id:
                print(f"Erro: Nível de Complexidade '{nivel}' não encontrado!")
                continue

            # Buscar o ID da Estratégia de Aprendizagem
            estrategia_id = conn.execute(
                text(
                    "SELECT ID FROM T_ESTRATEGIA_APRENDIZAGEM WITH(NOLOCK) "
                    "WHERE UPPER(DESCRICAO) = UPPER(:descricao)"
                ),
                {"descricao": row["ESTRATEGIA_APRENDIZAGEM"]}
            ).scalar()

            if not estrategia_id:
                print(
                  f"Erro: Estratégia de Aprendizagem "
                  f"'{row['ESTRATEGIA_APRENDIZAGEM']}' não encontrada!"
                )
                continue

            # Processar aulas
            aula_id = conn.execute(
                text(
                    "SELECT ID FROM T_AULAS WITH(NOLOCK) "
                    "WHERE TITULO=:titulo AND "
                    "FK_CAPITULO=:fk_capitulo AND "
                    "ORDEM=:ordem"
                ),
                {
                    "titulo": row["TITULO_AULA"],
                    "fk_capitulo": capitulo_id,
                    "ordem": row["ORDEM_AULA"]
                }
            ).scalar()

            if not aula_id:
                # Ler o conteúdo HTML para a aula
                nome_html = f"{row['LINK_CONTEUDO']}.html"
                conteudo_aula = ler_html(nome_html)

                aula_result = conn.execute(
                    text(
                        "EXEC sp_inserir_aula @titulo=:titulo, @ordem=:ordem,"
                        " @fk_capitulo=:fk_capitulo,"
                        " @fk_curso=:fk_curso, @fk_modulo=:fk_modulo,"
                        " @fk_tipo_aula=:fk_tipo_aula,"
                        " @palavras_chaves=:palavras_chaves,"
                        " @fk_nivel_complexidade=:fk_nivel_complexidade,"
                        " @conteudo_aula=:conteudo_aula,"
                        " @fk_estrategia_aprendizagem=:"
                        "fk_estrategia_aprendizagem,"
                        " @novo_id=:novo_id OUTPUT"
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
                print(f"Aula '{row['TITULO_AULA']}' inserida com ID {aula_id}")
            else:
                print(f"Aula '{row['TITULO_AULA']}' "
                      f"já existe com ID {aula_id}")

            # Inserir habilidades BNCC para cada código na coluna CODIGOS_BNCC
            codigos_bncc = row["CODIGOS_BNCC"].split(';') \
                if row["CODIGOS_BNCC"] else []
            bncc_habilidades = ";".join(
                [codigo.strip() for codigo in codigos_bncc]
            )

            # Inserir na T_BNCC_AULA com o índice da linha
            try:
                conn.execute(
                    text(
                        "EXEC sp_inserir_bncc_aula :fk_aula, "
                        ":bncc_habilidades, :linha"
                    ),
                    {
                        "fk_aula": aula_id,
                        "bncc_habilidades": bncc_habilidades,
                        "linha": idx + 1
                    }
                )
                print(f"Códigos BNCC inseridos para a aula ID "
                      f"{aula_id} na linha {idx + 1}")
            except Exception as e:
                print(f"Erro ao inserir códigos BNCC na linha {idx + 1}: {e}")
