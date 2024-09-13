from __future__ import annotations
import sys, json

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.Extrator import extrair_dados_sql
from Arquivos.ColetaDados.Tools import MainParameters


cnae_lista = MainParameters().cnae_analise()
cnae_sql = "|".join(f"{cnae}" for cnae in cnae_lista)


def extrair_caged(anos: list, cidades: list, save_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/562b56a3-0b01-4735-a049-eeac5681f056?table=95106d6f-e36e-4fed-b8e9-99c41cd99ecf
    query_caged = """
        SELECT
            ano,
            mes,
            dados.sigla_uf AS sigla_uf,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            dados.id_municipio_6 AS id_municipio_6,
            diretorio_id_municipio_6.nome AS id_municipio_6_nome,
            admitidos_desligados,
            tipo_estabelecimento,
            tipo_movimentacao_desagregado,
            faixa_emprego_inicio_janeiro,
            tempo_emprego,
            quantidade_horas_contratadas,
            salario_mensal,
            saldo_movimentacao,
            indicador_aprendiz,
            indicador_trabalho_intermitente,
            indicador_trabalho_parcial,
            indicador_portador_deficiencia,
            tipo_deficiencia,
            dados.cbo_2002 AS cbo_2002,
            diretorio_cbo_2002.descricao AS cbo_2002_descricao,
            diretorio_cbo_2002.descricao_familia AS cbo_2002_descricao_familia,
            diretorio_cbo_2002.descricao_subgrupo AS cbo_2002_descricao_subgrupo,
            diretorio_cbo_2002.descricao_subgrupo_principal AS cbo_2002_descricao_subgrupo_principal,
            diretorio_cbo_2002.descricao_grande_grupo AS cbo_2002_descricao_grande_grupo,
            dados.cnae_1 AS cnae_1,
            diretorio_cnae_1.descricao AS cnae_1_descricao,
            diretorio_cnae_1.descricao_grupo AS cnae_1_descricao_grupo,
            diretorio_cnae_1.descricao_divisao AS cnae_1_descricao_divisao,
            diretorio_cnae_1.descricao_secao AS cnae_1_descricao_secao,
            dados.cnae_2 AS cnae_2,
            diretorio_cnae_2.descricao_subclasse AS cnae_2_descricao_subclasse,
            diretorio_cnae_2.descricao_classe AS cnae_2_descricao_classe,
            diretorio_cnae_2.descricao_grupo AS cnae_2_descricao_grupo,
            diretorio_cnae_2.descricao_divisao AS cnae_2_descricao_divisao,
            diretorio_cnae_2.descricao_secao AS cnae_2_descricao_secao,
            cnae_2_subclasse,
            grau_instrucao,
            idade,
            sexo,
            raca_cor,
            regiao_corede,
            regiao_corede_04
        FROM `basedosdados.br_me_caged.microdados_antigos` AS dados
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN (SELECT DISTINCT id_municipio_6,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio_6
            ON dados.id_municipio_6 = diretorio_id_municipio_6.id_municipio_6
        LEFT JOIN (SELECT DISTINCT cbo_2002,descricao,descricao_familia,descricao_subgrupo,descricao_subgrupo_principal,descricao_grande_grupo  FROM `basedosdados.br_bd_diretorios_brasil.cbo_2002`) AS diretorio_cbo_2002
            ON dados.cbo_2002 = diretorio_cbo_2002.cbo_2002
        LEFT JOIN (SELECT DISTINCT cnae_1,descricao,descricao_grupo,descricao_divisao,descricao_secao  FROM `basedosdados.br_bd_diretorios_brasil.cnae_1`) AS diretorio_cnae_1
            ON dados.cnae_1 = diretorio_cnae_1.cnae_1
        LEFT JOIN (SELECT DISTINCT subclasse,descricao_subclasse,descricao_classe,descricao_grupo,descricao_divisao,descricao_secao  FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`) AS diretorio_cnae_2
            ON dados.cnae_2 = diretorio_cnae_2.subclasse


        WHERE 
                ano = {ano} 
                             
            """
    query_caged+= f"""AND (REGEXP_CONTAINS(dados.cnae_1, r'^({cnae_sql})') 
                        OR REGEXP_CONTAINS(dados.cnae_2, r'^({cnae_sql})'))\n"""

    processamento_caged = extrair_dados_sql(
    table_name= "caged",
    anos=anos,
    cidades=cidades,
    query_base=query_caged,
    save_dir=save_dir,
    ufs=ufs,
    mes = mes,
    limit=limit
    )

    return processamento_caged

