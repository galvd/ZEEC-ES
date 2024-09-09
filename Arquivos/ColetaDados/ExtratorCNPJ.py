from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql


cnae_lista = ['311601','311602','311603','311604','312401','312403','312404','321301','321302','321303','321304','321305','321399',
'322101','322102','322103','322104','322105','322106','322107','322199','600001','600002','600003','810001','810002',
'810003','810004','810005','810006','810007','810008','810010','810099','892401','892402','892403','893200','910600',
'1020101','1020102','2851800','3011301','3011302','3012100','3314714','3317101','3317102','4221901','4291000','4634603',
'4722902','4763604','4763605','5011401','5011402','5012201','5021101','5021102','5022001','5022002','5030101','5030102',
'5091201','5091202','5099801','5099899','5231101','5231102','5232000','5239700','5510801','5510802','5590601','5590602',
'5590603','5590699','7420002','7490102','7719501','7912100',]



def extrair_empresas(anos: list, cidades: list, save_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=b8432ff5-06c8-45ca-b8b6-33fceb24089d
    query_empresas = """
            WITH 
        dicionario_identificador_matriz_filial AS (
            SELECT
                chave AS chave_identificador_matriz_filial,
                valor AS descricao_identificador_matriz_filial
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'identificador_matriz_filial'
                AND id_tabela = 'estabelecimentos'
        ),
        dicionario_situacao_cadastral AS (
            SELECT
                chave AS chave_situacao_cadastral,
                valor AS descricao_situacao_cadastral
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'situacao_cadastral'
                AND id_tabela = 'estabelecimentos'
        ),
        dicionario_id_pais AS (
            SELECT
                chave AS chave_id_pais,
                valor AS descricao_id_pais
            FROM `basedosdados.br_me_cnpj.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'id_pais'
                AND id_tabela = 'estabelecimentos'
        )
        SELECT
            EXTRACT(YEAR FROM data) AS ano,
            EXTRACT(MONTH FROM data) AS mes,
            cnpj,
            descricao_identificador_matriz_filial AS identificador_matriz_filial,
            descricao_situacao_cadastral AS situacao_cadastral,
            data_situacao_cadastral,
            motivo_situacao_cadastral,
            nome_cidade_exterior,
            descricao_id_pais AS id_pais,
            data_inicio_atividade,
            cnae_fiscal_principal,
            cnae_fiscal_secundaria,
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            id_municipio_rf,
            tipo_logradouro,
            logradouro,
            numero,
            complemento,
            bairro,
            cep
        FROM `basedosdados.br_me_cnpj.estabelecimentos` AS dados
        LEFT JOIN `dicionario_identificador_matriz_filial`
            ON dados.identificador_matriz_filial = chave_identificador_matriz_filial
        LEFT JOIN `dicionario_situacao_cadastral`
            ON dados.situacao_cadastral = chave_situacao_cadastral
        LEFT JOIN `dicionario_id_pais`
            ON dados.id_pais = chave_id_pais
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio

            WHERE EXTRACT(YEAR FROM data) = {ano} 
                      
            """

    processamento_empresas = extrair_dados_sql(
    table_name= "cnpj_empresas",
    anos=anos,
    cidades=cidades,
    query_base=query_empresas,
    save_dir=save_dir,
    ufs=ufs,
    limit=limit
    )

    return processamento_empresas

