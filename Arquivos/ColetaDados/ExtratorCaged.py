from __future__ import annotations
import os
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

import basedosdados as bd



cloud_id = config['cloud_id']

def extrair_caged(anos:list, cidades:list, save_dir: str = None, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/3e7c4d58-96ba-448e-b053-d385a829ef00?table=86b69f96-0bfe-45da-833b-6edc9a0af213
    query_base = """
        WITH 
        dicionario_categoria AS (
            SELECT
                chave AS chave_categoria,
                valor AS descricao_categoria
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'categoria'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_grau_instrucao AS (
            SELECT
                chave AS chave_grau_instrucao,
                valor AS descricao_grau_instrucao
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'grau_instrucao'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_raca_cor AS (
            SELECT
                chave AS chave_raca_cor,
                valor AS descricao_raca_cor
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'raca_cor'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_sexo AS (
            SELECT
                chave AS chave_sexo,
                valor AS descricao_sexo
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'sexo'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_tipo_empregador AS (
            SELECT
                chave AS chave_tipo_empregador,
                valor AS descricao_tipo_empregador
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_empregador'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_tipo_estabelecimento AS (
            SELECT
                chave AS chave_tipo_estabelecimento,
                valor AS descricao_tipo_estabelecimento
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_estabelecimento'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_tipo_movimentacao AS (
            SELECT
                chave AS chave_tipo_movimentacao,
                valor AS descricao_tipo_movimentacao
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_movimentacao'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_tipo_deficiencia AS (
            SELECT
                chave AS chave_tipo_deficiencia,
                valor AS descricao_tipo_deficiencia
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_deficiencia'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_indicador_trabalho_intermitente AS (
            SELECT
                chave AS chave_indicador_trabalho_intermitente,
                valor AS descricao_indicador_trabalho_intermitente
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'indicador_trabalho_intermitente'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_indicador_trabalho_parcial AS (
            SELECT
                chave AS chave_indicador_trabalho_parcial,
                valor AS descricao_indicador_trabalho_parcial
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'indicador_trabalho_parcial'
                AND id_tabela = 'microdados_movimentacao'
        ),
        dicionario_indicador_aprendiz AS (
            SELECT
                chave AS chave_indicador_aprendiz,
                valor AS descricao_indicador_aprendiz
            FROM `basedosdados.br_me_caged.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'indicador_aprendiz'
                AND id_tabela = 'microdados_movimentacao'
        )
        SELECT
            ano,
            mes,
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            cnae_2_secao,
            cnae_2_subclasse,
            saldo_movimentacao,
            dados.cbo_2002 AS cbo_2002,
            diretorio_cbo_2002.descricao AS cbo_2002_descricao,
            diretorio_cbo_2002.descricao_familia AS cbo_2002_descricao_familia,
            diretorio_cbo_2002.descricao_subgrupo AS cbo_2002_descricao_subgrupo,
            diretorio_cbo_2002.descricao_subgrupo_principal AS cbo_2002_descricao_subgrupo_principal,
            diretorio_cbo_2002.descricao_grande_grupo AS cbo_2002_descricao_grande_grupo,
            descricao_categoria AS categoria,
            descricao_grau_instrucao AS grau_instrucao,
            idade,
            horas_contratuais,
            descricao_raca_cor AS raca_cor,
            descricao_sexo AS sexo,
            descricao_tipo_empregador AS tipo_empregador,
            descricao_tipo_estabelecimento AS tipo_estabelecimento,
            descricao_tipo_movimentacao AS tipo_movimentacao,
            descricao_tipo_deficiencia AS tipo_deficiencia,
            descricao_indicador_trabalho_intermitente AS indicador_trabalho_intermitente,
            descricao_indicador_trabalho_parcial AS indicador_trabalho_parcial,
            salario_mensal,
            descricao_indicador_aprendiz AS indicador_aprendiz
        FROM `basedosdados.br_me_caged.microdados_movimentacao` AS dados
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN (SELECT DISTINCT cbo_2002,descricao,descricao_familia,descricao_subgrupo,descricao_subgrupo_principal,descricao_grande_grupo  FROM `basedosdados.br_bd_diretorios_brasil.cbo_2002`) AS diretorio_cbo_2002
            ON dados.cbo_2002 = diretorio_cbo_2002.cbo_2002
        LEFT JOIN `dicionario_categoria`
            ON dados.categoria = chave_categoria
        LEFT JOIN `dicionario_grau_instrucao`
            ON dados.grau_instrucao = chave_grau_instrucao
        LEFT JOIN `dicionario_raca_cor`
            ON dados.raca_cor = chave_raca_cor
        LEFT JOIN `dicionario_sexo`
            ON dados.sexo = chave_sexo
        LEFT JOIN `dicionario_tipo_empregador`
            ON dados.tipo_empregador = chave_tipo_empregador
        LEFT JOIN `dicionario_tipo_estabelecimento`
            ON dados.tipo_estabelecimento = chave_tipo_estabelecimento
        LEFT JOIN `dicionario_tipo_movimentacao`
            ON dados.tipo_movimentacao = chave_tipo_movimentacao
        LEFT JOIN `dicionario_tipo_deficiencia`
            ON dados.tipo_deficiencia = chave_tipo_deficiencia
        LEFT JOIN `dicionario_indicador_trabalho_intermitente`
            ON dados.indicador_trabalho_intermitente = chave_indicador_trabalho_intermitente
        LEFT JOIN `dicionario_indicador_trabalho_parcial`
            ON dados.indicador_trabalho_parcial = chave_indicador_trabalho_parcial
        LEFT JOIN `dicionario_indicador_aprendiz`
            ON dados.indicador_aprendiz = chave_indicador_aprendiz

        WHERE 
                sigla_uf = "ES" 
                AND diretorio_id_municipio.nome IN ({cidades})
                AND ano = {ano}
            {limit}
            """
    
    cidades_sql = ", ".join(f"'{cidade}'" for cidade in cidades)
    df_matriz = []

    for ano in anos:
        query = query_base.format(ano=ano, cidades=cidades_sql, limit=limit)
        table_caged = bd.read_sql(query=query, billing_project_id=cloud_id)

        print(f"Dados do CAGED de {ano} processados com sucesso!")
        df_matriz.append(table_caged)

        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            
            # Define o caminho completo do arquivo CSV e o temporário
            file_path = os.path.join(save_dir + "\\Dados\\CAGED", f'caged_{ano}.csv')
            temp_file_path = os.path.join(save_dir + "\\Dados\\CAGED", f'caged_{ano}_temp.csv')
            
            # Salva o DataFrame no arquivo temporário
            table_caged.to_csv(temp_file_path, index=False)
            print(f"Arquivo temporário salvo em: {temp_file_path}")
            
            # Verifica se o arquivo original já existe e o remove
            if os.path.exists(file_path):
                print(f"Arquivo original encontrado. Removendo: {file_path}")
                os.remove(file_path)
            
            # Renomeia o arquivo temporário para o nome final
            os.rename(temp_file_path, file_path)
            print(f"Arquivo final salvo em: {file_path}")
    print("Processamento de dados do CAGED completo!")
        
    # Retorna todos os resultados como lista de dataframes
    return df_matriz