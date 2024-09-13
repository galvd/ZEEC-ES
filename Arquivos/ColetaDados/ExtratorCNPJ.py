from __future__ import annotations
from datetime import datetime
from glob import glob
import json, sys, os
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])


from Arquivos.ColetaDados.Extrator import extrair_dados_sql, extrair_cnpj_url
from Arquivos.ColetaDados.Tools import MainParameters, CnpjTreatment


cnae_lista = MainParameters().cnae_analise()
cnae_sql = "|".join(f"{cnae}" for cnae in cnae_lista)


def extrair_empresas_bd(anos: list, cidades: list, save_dir: str = None, ufs: str = "", mes: int = None, limit: str = ""):

    

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

        WHERE 
            EXTRACT(YEAR FROM data) = {ano}             

            """
    query_empresas+= f"""AND (REGEXP_CONTAINS(cnae_fiscal_principal, r'^({cnae_sql})') 
                        OR REGEXP_CONTAINS(cnae_fiscal_secundaria, r'^({cnae_sql})'))\n"""

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



def extrair_empresas_url(url_template: str, save_dir: str, anos: list = []):
    treat = CnpjTreatment()
    dir = os.path.join(save_dir, "Dados", "Cnpj Empresas")
    print('Iniciando download dos arquivos de CNPJs da Receita Federal')

    mes_atual = datetime.today().month
    lista_meses = [f"{mes:02d}" for mes in range(mes_atual-4, mes_atual)]

    def limpar_residuo():
        # Path dos arquivos que serão deletados após a conclusão do mês
        csv_to_del = glob(os.path.join(dir, r'*.ESTABELE'))
        zip_to_del = glob(os.path.join(dir, fr'*{ano}_{mes}.zip'))
        tmp_to_del = glob(os.path.join(dir, r'*.tmp'))

        list(map(os.remove, csv_to_del))
        list(map(os.remove, zip_to_del))
        list(map(os.remove, tmp_to_del))

        
    #loop em anos, pegar meses com datetime
    for ano in anos:
        if ano > datetime.today().year:
            return print(f'Ano de {ano} inexistente no repositório. Processo finalizado.')
        for mes in lista_meses:
            
        
            # Checando se o parquet objetivo do ano_mes já foi gerado
            parquet_final = os.path.exists(os.path.join(dir, f'cnpjs_{ano}_{mes}.parquet'))

            if parquet_final:
                print(f'O arquivo objetivo cnpjs_{ano}_{mes}.parquet já foi gerado, removendo arquivos e indo para o próximo mês')

                # deleta zips e csv
                limpar_residuo()
                
                continue
            
            for file_count in range(0,10):
                print(f"Processando {ano}-{mes} (Count: {file_count})")
                url_formatada = url_template.format(ano=ano, mes=mes)

                    # Baixar e renomear o arquivo
                zip_name = extrair_cnpj_url(url=url_formatada, dir=dir, ano=ano, mes=mes, count= file_count)
                
                # Descompactar o arquivo
                treat.unzip_cnpj(dir=dir, zip_name=zip_name, ano=ano, mes=mes)

            # Converter para Parquet
            treat.shrink_to_parquet(dir=dir, ano=ano, mes=mes)
            
            print('Excluindo arquivos após exportar para parquet')

            # deleta zips e csv
            limpar_residuo()

    print('Downloads e transformações dos dados de CNPJ finalizados.')


from os import getcwd
url = 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/{ano}-{mes}/'
proj_dir = getcwd()
extrair_empresas_url(url_template= url, save_dir=proj_dir, anos = [2024, 2025])

    

def extrair_munic_dict(save_dir: str = None):
    # Query que gera o dicionário com nome, código IBGE, código SERPRO (fiscal da RF) dos municípios do Brasil

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=b8432ff5-06c8-45ca-b8b6-33fceb24089d
    query_munic = """
            WITH 
            dicionario_identificador_matriz_filial AS (
                SELECT
                    chave AS chave_identificador_matriz_filial,
                    valor AS descricao_identificador_matriz_filial
                FROM `basedosdados.br_me_cnpj.dicionario`
                WHERE
                    nome_coluna = 'identificador_matriz_filial'
                    AND id_tabela = 'estabelecimentos'
            ),
            dicionario_situacao_cadastral AS (
                SELECT
                    chave AS chave_situacao_cadastral,
                    valor AS descricao_situacao_cadastral
                FROM `basedosdados.br_me_cnpj.dicionario`
                WHERE
                    nome_coluna = 'situacao_cadastral'
                    AND id_tabela = 'estabelecimentos'
            ),
            dicionario_id_pais AS (
                SELECT
                    chave AS chave_id_pais,
                    valor AS descricao_id_pais
                FROM `basedosdados.br_me_cnpj.dicionario`
                WHERE
                    nome_coluna = 'id_pais'
                    AND id_tabela = 'estabelecimentos'
            )
        SELECT
            DISTINCT
            dados.id_municipio_rf AS id_municipio_rf,
            diretorio_id_municipio.nome AS id_municipio_nome,
            dados.id_municipio AS id_municipio,
            dados.sigla_uf AS sigla_uf
        FROM `basedosdados.br_me_cnpj.estabelecimentos` AS dados
        LEFT JOIN `dicionario_identificador_matriz_filial`
            ON dados.identificador_matriz_filial = chave_identificador_matriz_filial
        LEFT JOIN `dicionario_situacao_cadastral`
            ON dados.situacao_cadastral = chave_situacao_cadastral
        LEFT JOIN `dicionario_id_pais`
            ON dados.id_pais = chave_id_pais
        LEFT JOIN (
            SELECT DISTINCT id_municipio, nome  
            FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        ) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio;
            """

    processamento_munic = extrair_dados_sql(
    table_name= "cods_municipios",
    anos = [2024],
    query_base=query_munic,
    save_dir=save_dir
    )

    return processamento_munic






