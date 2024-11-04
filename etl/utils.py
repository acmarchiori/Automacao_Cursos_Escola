from sqlalchemy.sql import text

def processar_cursos(df):
    # Filtrar colunas necessárias para a tabela de cursos
    cursos = df[["ANO", "SEGMENTO", "ÁREA"]].drop_duplicates()
    cursos.rename(
        columns={
            "ANO": "ANO_ESCOLAR",
            "SEGMENTO": "SEGMENTO_ESCOLAR",
            "ÁREA": "TITULO",
        },
        inplace=True,
    )

    # Adicionar colunas fixas com parâmetros
    cursos["COR"] = "#EB7D36"
    cursos["TIPO_CURSO"] = "ESCOLAR"
    cursos["AREA_BNCC"] = "LP"
    return cursos

def inserir_cursos(engine, cursos_df):
    with engine.begin() as conn:
        for index, row in cursos_df.iterrows():
            # Depuração: Imprimindo valores que estamos buscando
            print(f"Buscando segmento escolar: {row['SEGMENTO_ESCOLAR']}")
            print(f"Buscando ano escolar: {row['ANO_ESCOLAR']}")
            # Ajustando a busca para considerar nomes exatos
            segmento_id = conn.execute(
                text("SELECT ID FROM T_SEGMENTOS_ESCOLARES WHERE NOME=:nome"),
                {"nome": row["SEGMENTO_ESCOLAR"]}
            ).scalar()

            ano_id = conn.execute(
                text(
                    "SELECT ID FROM T_ANOS_ESCOLARES WHERE NOME=:nome AND "
                    "FK_SEGMENTO_ESCOLAR=:fk_segmento"
                ),
                {"nome": row["ANO_ESCOLAR"], "fk_segmento": segmento_id}
            ).scalar()

            area_bncc_id = conn.execute(
                text("SELECT ID FROM T_AREAS_BNCC WHERE CODIGO=:codigo"),
                {"codigo": "LP"}  # Utilize o código direto se for constante
            ).scalar()

            # Depuração: Imprimindo IDs encontrados
            print(f"ID do segmento escolar: {segmento_id}")
            print(f"ID do ano escolar: {ano_id}")
            print(f"ID da área BNCC: {area_bncc_id}")

            if not segmento_id or not ano_id or not area_bncc_id:
                print(
                    (
                        "Erro: Segmento, Ano escolar ou Área BNCC não encontrado "
                        f"para o curso {row['TITULO']}"
                    )
                )
                continue

            result = conn.execute(
                text(
                    "EXEC sp_inserir_curso @titulo=:titulo, @tipo_curso=:tipo_curso,"
                    " @cor=:cor, @segmento_escolar=:segmento_escolar, @ano_escolar="
                    ":ano_escolar, @area_bncc=:area_bncc, @novo_id=:novo_id OUTPUT"
                ),
                {
                    "titulo": row["TITULO"],
                    "tipo_curso": row["TIPO_CURSO"],
                    "cor": row["COR"],
                    "segmento_escolar": segmento_id,
                    "ano_escolar": ano_id,
                    "area_bncc": area_bncc_id,
                    "novo_id": 0,  # Inicialize o valor de novo_id
                },
            )
            novo_id = result.scalar()
            # Captura o ID gerado para uso nas FK dos próximos passos
            print(f"Curso {row['TITULO']} inserido com ID {novo_id}")

            # Verificação de inserção
            verif_curso = conn.execute(
                text("SELECT * FROM T_CURSOS WHERE TITULO=:titulo"),
                {"titulo": row["TITULO"]}
            ).fetchone()
            if verif_curso:
                print(
                    f"Verificado: Curso {verif_curso.TITULO} está na tabela "
                    f"T_CURSOS com ID {verif_curso.ID}."
                )
            else:
                print(
                    f"Erro: Curso {row['TITULO']} não encontrado na tabela "
                    "T_CURSOS após inserção."
                )
