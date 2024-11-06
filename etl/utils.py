from sqlalchemy.sql import text

# Defina as variáveis de tipo de curso e tipo de aula no início
TIPO_CURSO = "ESCOLAR"
TIPO_AULA = "CONTEÚDO EM TEXTO E IMAGENS"
AREA_BNCC = "LP"
COR = "#EB7D36"


def processar_cursos(df):
    cursos = df[[
        "ANO_ESCOLAR", "SEGMENTO_ESCOLAR", "TITULO",
        "ORDEM_MODULO", "NOME_MODULO", "ORDEM_CAPITULO",
        "NOME_CAPITULO", "ORDEM_AULA", "TITULO_AULA"
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
        for _, row in cursos_df.iterrows():
            # Buscar IDs necessários
            segmento_id = conn.execute(
                text("SELECT ID FROM T_SEGMENTOS_ESCOLARES WITH(NOLOCK) WHERE NOME=:nome"),
                {"nome": row["SEGMENTO_ESCOLAR"]}
            ).scalar()

            ano_id = conn.execute(
                text(
                    "SELECT ID FROM T_ANOS_ESCOLARES WITH(NOLOCK) WHERE NOME=:nome AND "
                    "FK_SEGMENTO_ESCOLAR=:fk_segmento"
                ),
                {"nome": row["ANO_ESCOLAR"], "fk_segmento": segmento_id}
            ).scalar()

            area_bncc_id = conn.execute(
                text("SELECT ID FROM T_AREAS_BNCC WITH(NOLOCK) WHERE CODIGO=:codigo"),
                {"codigo": "LP"}
            ).scalar()

            if not segmento_id or not ano_id or not area_bncc_id:
                print(f"Erro: Segmento, Ano escolar ou Área BNCC não encontrado para o curso {row['TITULO']}")
                continue

            curso_id = conn.execute(
                text("SELECT ID FROM T_CURSOS WITH(NOLOCK) WHERE TITULO=:titulo"),
                {"titulo": row["TITULO"]}
            ).scalar()

            if not curso_id:
                result = conn.execute(
                    text(
                        "EXEC sp_inserir_curso @titulo=:titulo, @tipo_curso=:tipo_curso,"
                        " @cor=:cor, @segmento_escolar=:segmento_escolar, @ano_escolar=:ano_escolar,"
                        " @area_bncc=:area_bncc, @novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "titulo": row["TITULO"],
                        "tipo_curso": TIPO_CURSO,  # Usando a variável definida
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
                    "SELECT ID FROM T_MODULOS WITH(NOLOCK) WHERE NOME=:nome AND FK_CURSO=:fk_curso"
                ),
                {"nome": row["NOME_MODULO"], "fk_curso": curso_id}
            ).scalar()

            if not modulo_id:
                modulo_result = conn.execute(
                    text(
                        "EXEC sp_inserir_modulo @nome=:nome, @ordem=:ordem, @fk_curso=:fk_curso, @novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "nome": row["NOME_MODULO"],
                        "ordem": row["ORDEM_MODULO"],
                        "fk_curso": curso_id,
                        "novo_id": 0,
                    },
                )
                modulo_id = modulo_result.scalar()
                print(f"Módulo '{row['NOME_MODULO']}' inserido com ID {modulo_id}")
            else:
                print(f"Módulo '{row['NOME_MODULO']}' já existe com ID {modulo_id}")

            modulos_inseridos[modulo_chave] = modulo_id

            # Processar capítulos
            capitulo_chave = (modulo_id, row["NOME_CAPITULO"], row["ORDEM_CAPITULO"])
            capitulo_id = capitulos_inseridos.get(capitulo_chave) or conn.execute(
                text(
                    "SELECT ID FROM T_CAPITULOS WITH(NOLOCK) WHERE NOME=:nome AND FK_MODULO=:fk_modulo"
                ),
                {"nome": row["NOME_CAPITULO"], "fk_modulo": modulo_id}
            ).scalar()

            if not capitulo_id:
                capitulo_result = conn.execute(
                    text(
                        "EXEC sp_inserir_capitulo @nome=:nome, @ordem=:ordem, @fk_modulo=:fk_modulo, @novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "nome": row["NOME_CAPITULO"],
                        "ordem": row["ORDEM_CAPITULO"],
                        "fk_modulo": modulo_id,
                        "novo_id": 0,
                    },
                )
                capitulo_id = capitulo_result.scalar()
                print(f"Capítulo '{row['NOME_CAPITULO']}' inserido com ID {capitulo_id}")
            else:
                print(f"Capítulo '{row['NOME_CAPITULO']}' já existe com ID {capitulo_id}")

            capitulos_inseridos[capitulo_chave] = capitulo_id

            # Obter o ID de T_TIPO_AULA para 'CONTEÚDO EM TEXTO E IMAGEM'
            tipo_aula_id = conn.execute(
                text("SELECT ID FROM T_TIPO_AULA WITH(NOLOCK) WHERE NOME=:nome"),
                {"nome": TIPO_AULA}
            ).scalar()

            if not tipo_aula_id:
                print(f"Erro: Tipo de Aula '{TIPO_AULA}' não encontrado!")
                continue

            # Processar aulas
            aula_id = conn.execute(
                text(
                    "SELECT ID FROM T_AULAS WITH(NOLOCK) WHERE TITULO=:titulo AND FK_CAPITULO=:fk_capitulo"
                ),
                {"titulo": row["TITULO_AULA"], "fk_capitulo": capitulo_id}
            ).scalar()

            if not aula_id:
                aula_result = conn.execute(
                    text(
                        "EXEC sp_inserir_aula @titulo=:titulo, @ordem=:ordem, @fk_capitulo=:fk_capitulo,"
                        " @fk_curso=:fk_curso, @fk_modulo=:fk_modulo, @fk_tipo_aula=:fk_tipo_aula, @novo_id=:novo_id OUTPUT"
                    ),
                    {
                        "titulo": row["TITULO_AULA"],
                        "ordem": row["ORDEM_AULA"],
                        "fk_capitulo": capitulo_id,
                        "fk_curso": curso_id,  # Passando o fk_curso aqui
                        "fk_modulo": modulo_id,  # Passando o fk_modulo
                        "fk_tipo_aula": tipo_aula_id,  # Usando o ID de T_TIPO_AULA
                        "novo_id": 0,
                    },
                )
                aula_id = aula_result.scalar()
                print(f"Aula '{row['TITULO_AULA']}' inserida com ID {aula_id}")
            else:
                print(f"Aula '{row['TITULO_AULA']}' já existe com ID {aula_id}")
