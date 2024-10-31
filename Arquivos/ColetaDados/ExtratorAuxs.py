'''
Extrator de arquivos auxiliares como dados de dimensão dos municipios, dicionários de datasets, ceps, etc.
'''


from __future__ import annotations
import sys, json, os
import pandas as pd


with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.ToolsColeta import download_overwrite
from Arquivos.ColetaDados.Extrator import extrair_dados_sql



def extrair_munic_dict(main_dir: str = None):
    # Query que gera o dicionário com nome, código IBGE, código (fiscal da RF) SERPRO  dos municípios do Brasil
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
    main_dir=main_dir
    )

    dict_path = os.path.join(main_dir, 'Dados', 'Cods Municipios')

    df_mun = pd.read_parquet(os.path.join(dict_path, 'cods_municipios_2024.parquet'))

    df_mun['id_municipio_ibge'] = df_mun['id_municipio'].astype(str).str.replace("\.0", "")
    df_mun.drop(columns=['id_municipio'])

    df_mun['id_municipio_nome']['Atilio Vivacqua'] = 'Atilio Vivácqua'

    print(df_mun)

    url = r'https://drive.google.com/uc?id=1Uy-VBKChb6VAaY8DAOb7hqmUhNGzu7aR' # link com os dados originais: https://www.ibge.gov.br/cidades-e-estados/es.html; Exportar > Todos os Municípios > Baixar xlsx
    download_overwrite(dw_path= dict_path, file_name='area_cidades_es.xlsx', url=url)

    keep_columns = ['Município [-]','Código [-]','Área Territorial - km² [2022]',
                            'População residente - pessoas [2022]','Densidade demográfica - hab/km² [2022]']
    schema = dict(zip(keep_columns, [str, str, float, float, float]))

    df_area = pd.read_excel(os.path.join(dict_path, f'area_cidades_es.xlsx'),
                        index_col = False, header=2, dtype=schema)[keep_columns]

    cols_rename = dict(zip(keep_columns, ['id_municipio_nome', 'id_municipio_ibge', 'area_mun_km2', 'pop2022', 'densid_2022']))
    df_area.rename(columns=cols_rename, inplace=True)
    
    df_area['id_municipio_nome']['Atilio Vivacqua'] = 'Atilio Vivácqua'
    df_area['id_municipio_ibge'] = df_area['id_municipio_ibge'].astype(str)
    df_area['id_municipio_ibge'] = df_area['id_municipio_ibge'].replace('.0', '')

    df = df_mun.merge(df_area, how='inner', on='id_municipio_ibge')

    df['id_municipio_rf'] = df['id_municipio_rf'].astype(str)
    df['id_municipio_ibge'] = df['id_municipio_ibge'].astype(str)
    df = df.drop(columns=['id_municipio', 'id_municipio_nome_x']) \
           .rename(columns={'id_municipio_nome_y': 'id_municipio_nome'})
    
    df.to_parquet(os.path.join(dict_path, 'cidadeES.parquet'), index= False)

def extrair_cnpj_dict(main_dir: str = None):
    # Query que gera o dicionário dos códigos de caracterização dos dados de CNPJ da Receita Federal
    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/e43f0d5b-43cf-4bfb-8d90-c38a4e0d7c4f?table=3dbb38d1-65af-44a3-b43a-7b088891ebc0
    query_cnpj_dict = """
            SELECT
            *
            FROM `basedosdados.br_me_cnpj.dicionario`
            """

    processamento_munic = extrair_dados_sql(
    table_name= "dict_cnpjs",
    query_base=query_cnpj_dict,
    main_dir=main_dir
    )

extrair_cnpj_dict(os.getcwd())

def get_poligono_bairros_es(main_dir: str):
    url = r'https://ide.geobases.es.gov.br/geoserver/ows?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3Aijsn_limite_bairro_2020_UTF8&outputFormat=csv&srs=EPSG%3A31984'
    csv_path = os.path.join(main_dir, 'Dados')
    csv_name = 'area_bairros_es.csv'

    download_overwrite(dw_path=csv_path, file_name=csv_name, url=url)
    df_regions = pd.read_csv(os.path.join(main_dir, 'Dados', f'area_bairros_es.csv'))
    df_regions.drop(columns = ['origem', 'anoReferen', 'fonte', 'distrito', 'FID', 'fid', 'the_geom', 'OBJECTID',
                               'geocodigo',	'situacao',	'geocDistr', 'geocMun', 'escala', 'Shape_Leng', 'Shape_Area'], 
                               inplace= True)
    df_regions.rename({'nome': 'bairro', 'municipio': 'id_municipio_nome', 'areaM2': 'area_m2'}, axis= 1,  inplace=True)
    df_regions.to_csv(os.path.join(main_dir, 'Dados', f'bairros_es.csv'), index= False)

def get_cnae_dict(main_dir:str):
    url = r'https://concla.ibge.gov.br/images/concla/documentacao/CNAE_Subclasses_2_3_Estrutura_Detalhada.xlsx'
    xl_path = os.path.join(main_dir, 'Dados')
    xl_name = 'cnae_dict.xlsx'
    dtypes = {'Seção': str, 'Divisão': str, 'Grupo': str,
              'Classe': str, 'Subclasse': str, '': str}
    cols_rename = {'Seção':'cnae_secao', 'Divisão':'cnae_divisao', 'Grupo':'cnae_grupo',
                   'Classe':'cnae_classe', 'Subclasse':'cnae_fiscal', 'Unnamed: 5':'cnae_descri'}
    
    download_overwrite(dw_path=xl_path, file_name=xl_name, url=url)
    # Lendo o arquivo Excel com os tipos definidos
    cnaes = pd.read_excel(os.path.join(xl_path, xl_name), dtype=dtypes, header=3).rename(columns=cols_rename)

    cnaes['cnae_descri'] = cnaes['cnae_descri'].str.title()
    cnaes['cnae_grupo'] = cnaes['cnae_grupo'].str.replace('\.', '')
    cnaes['cnae_classe'] = cnaes['cnae_classe'].str.replace('\-', '').str.replace('\.', '')
    cnaes['cnae_fiscal'] = cnaes['cnae_fiscal'].str.replace('\/', '').str.replace('\-', '')

    # 1. Substituir valores não nulos nas colunas cnae_secao, cnae_divisao, cnae_grupo e cnae_classe
    cols_to_replace = ['cnae_secao', 'cnae_divisao', 'cnae_grupo', 'cnae_classe']
    for col in cols_to_replace:
        cnaes.loc[cnaes[col].notna(), col] = cnaes.loc[cnaes[col].notna(), 'cnae_descri']

    # 2. Aplicar o ffill para preencher os valores NaN
    cnaes = cnaes.ffill()

    # 3. Restaurar os NaNs para manter a hierarquia correta
    cnaes.loc[cnaes['cnae_classe'] != cnaes['cnae_classe'].shift(), ['cnae_fiscal']] = None
    cnaes.loc[cnaes['cnae_grupo'] != cnaes['cnae_grupo'].shift(), ['cnae_classe', 'cnae_fiscal']] = None
    cnaes.loc[cnaes['cnae_divisao'] != cnaes['cnae_divisao'].shift(), ['cnae_grupo', 'cnae_classe', 'cnae_fiscal']] = None
    cnaes.loc[cnaes['cnae_secao'] != cnaes['cnae_secao'].shift(), ['cnae_divisao', 'cnae_grupo', 'cnae_classe', 'cnae_fiscal']] = None

    cnaes = cnaes.dropna(subset='cnae_fiscal')

    cnaes.to_parquet(os.path.join(xl_path, f'cnae_dict.parquet'), index=False)


