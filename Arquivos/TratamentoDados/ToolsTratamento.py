"""
ToolsTratamento.py

Este arquivo define a classe CnpjProcess e funções auxiliares para o processamento de dados de CNPJ, incluindo conversão de arquivos CSV para Parquet e processamento paralelo.

Imports
Bibliotecas Padrão:

os
json
sys
glob
concurrent.futures
multiprocessing.pool
multiprocessing
Bibliotecas de Dados:

pandas as pd
dask.dataframe as dd
Dataclasses:

from dataclasses import dataclass
Módulo para Processamento Paralelo:

from multiprocessing.pool import Pool
from multiprocessing import cpu_count
Classes e Métodos
1. CnpjProcess
A classe CnpjProcess é responsável pelo tratamento e conversão de dados de CNPJ, incluindo a leitura de arquivos CSV, filtragem, e conversão para o formato Parquet.

Atributos:

treat: Instância da classe CnpjTreatment, utilizada para operações de coleta de dados.
Métodos:

shrink_to_parquet(self, cnpj_dir: str, ano: str, mes: str, count: int, cidades: list, ufs: list)

Descrição: Converte arquivos CSV para o formato Parquet após aplicar filtros específicos.
Parâmetros:
cnpj_dir: Diretório onde os arquivos CSV estão localizados.
ano: Ano dos dados.
mes: Mês dos dados.
count: Contador para identificar os arquivos.
cidades: Lista de cidades para filtrar os dados.
ufs: Lista de Unidades Federativas para filtrar os dados.
Passos:
Verifica se a pasta Parquet já existe.
Lê os arquivos CSV e aplica filtros.
Converte os dados filtrados para o formato Parquet e salva.
unify_parquet(self, ano: str, mes: str, cnpj_dir: str, count: int = 99)

Descrição: Unifica arquivos Parquet individuais em um arquivo final.
Parâmetros:
ano: Ano dos dados.
mes: Mês dos dados.
cnpj_dir: Diretório onde os arquivos Parquet estão localizados.
count: Contador para identificar os arquivos.
Passos:
Tenta ler e unificar os arquivos Parquet.
Em caso de erro, exibe uma mensagem.
limpar_residuo(self, ano: str, mes: str, cnpj_dir: str)

Descrição: Remove arquivos temporários e desnecessários após o processamento.
Parâmetros:
ano: Ano dos dados.
mes: Mês dos dados.
cnpj_dir: Diretório onde os arquivos estão localizados.
Passos:
Remove arquivos CSV, ZIP e temporários.
core_tasks_mt(self, count: int, url_formatada: str, cnpj_dir: str, ano: str, mes: str, file_count: int, cidades: list, ufs: list)

Descrição: Executa tarefas principais de download, descompactação e processamento dos dados em paralelo.
Parâmetros:
count: Contador para identificar os arquivos.
url_formatada: URL formatada para o download dos arquivos.
cnpj_dir: Diretório onde os arquivos serão salvos.
ano: Ano dos dados.
mes: Mês dos dados.
file_count: Número total de arquivos a serem processados.
cidades: Lista de cidades para filtrar os dados.
ufs: Lista de Unidades Federativas para filtrar os dados.
Passos:
Executa download, descompactação e conversão para Parquet.
core_tasks_dt(self, downloader: function, processer: function, iterations: int)

Descrição: Gerencia o processamento paralelo com funções de download e processamento.
Parâmetros:
downloader: Função responsável pelo download dos arquivos.
processer: Função responsável pelo processamento dos arquivos.
iterations: Número de iterações para o processamento.
Passos:
Organiza a execução em etapas usando um ThreadPoolExecutor.
Funções Auxiliares
one_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, cidades: list, ufs: list)

Descrição: Executa tarefas de download, descompactação e conversão em um único thread.
Parâmetros: Igual à core_tasks_mt.
dual_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, cidades: list, ufs: list)

Descrição: Executa tarefas de download e processamento em dois threads simultaneamente.
Parâmetros: Igual à core_tasks_mt.
multi_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, cidades: list, ufs: list)

Descrição: Executa tarefas de download e processamento em múltiplos threads utilizando o Pool do módulo multiprocessing.
Parâmetros: Igual à core_tasks_mt.
"""



from __future__ import annotations
import os, concurrent.futures, sys, json, unicodedata
import dask.dataframe as dd
import polars as pl
import pandas as pd
from dataclasses import dataclass
from glob import glob
from multiprocessing.pool import Pool
from multiprocessing import cpu_count

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])

from Arquivos.ColetaDados.ToolsColeta import CnpjTreatment, MainParameters



@dataclass
class CnpjProcess:

    treat = CnpjTreatment()

    # Função que lê a pasta onde os arquivos foram descompactados por unzip_cnpj, coleta apenas as colunas e linhas de interesse e salva a tabela tratada em .parquet
    def shrink_to_parquet(self, cnpj_dir: str, ano: str, mes: str, count: int, cidades: list, ufs:list, fonte: str):
        param = MainParameters()
        cnae_lista = param.cnae_analise()
        parquet_mid = f"cnpj_{ano}_{mes}_{count}"

        print('Iniciando conversão e tratamento de csv para parquet')

        # Função para verificar se o csv já foi transformado em parquets
        etapa_atual = self.treat.etapa_atual(ano, mes, cnpj_dir, count, fonte)
        if etapa_atual == 'pasta_parquet':
            return print(f'Pasta de parquets já existente: {parquet_mid} \nPulando para próxima etapa.')

        colunas_estabelecimento = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
                                    'nome_fantasia',
                                    'situacao_cadastral','data_situacao_cadastral', 
                                    'motivo_situacao_cadastral',
                                    'nome_cidade_exterior',
                                    'pais',
                                    'data_inicio_atividades',
                                    'cnae_fiscal',
                                    'cnae_fiscal_secundaria',
                                    'tipo_logradouro',
                                    'logradouro', 
                                    'numero',
                                    'complemento','bairro',
                                    'cep','uf','municipio',
                                    'ddd1', 'telefone1',
                                    'ddd2', 'telefone2',
                                    'ddd_fax', 'fax',
                                    'correio_eletronico',
                                    'situacao_especial',
                                    'data_situacao_especial']  
                            
        colunas_empresas = ['cnpj_basico', 
                    'razao_social', 
                    'natureza_jur', 
                    'quali_responsavel', 
                    'capital_social', 
                    'porte', 
                    'ente_responsavel'
                    ]
        
        
        if fonte == 'Empresas':
            colunas = colunas_empresas
            arq_descompactados = glob(os.path.join(cnpj_dir, fr'*Y{count}.*.EMPRECSV'))
        if fonte == 'Estabelecimentos':
            colunas = colunas_estabelecimento
            arq_descompactados = glob(os.path.join(cnpj_dir, fr'*Y{count}.*.ESTABELE'))
        

        print('Iniciando leitura dos csv para conversão')
        for arq in arq_descompactados:
            print(f'Lendo o csv n{count}' + arq)

            df = pl.read_csv(source= arq, separator=';', has_header= False, 
                             new_columns= colunas, encoding='latin1', 
                             schema_overrides=[pl.String], null_values= None)

            print('csv lido e colunas fora do escopo retiradas')

            if fonte == 'Estabelecimentos':
                df = df[['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
                            'situacao_cadastral','data_situacao_cadastral', 
                            'motivo_situacao_cadastral',
                            'data_inicio_atividades',
                            'cnae_fiscal',
                            'cnae_fiscal_secundaria',
                            'tipo_logradouro',
                            'logradouro', 
                            'numero',
                            'complemento','bairro',
                            'cep','uf','municipio']]
                # Cria uma máscara para filtrar os CNAEs primários
                mask_primary = df['cnae_fiscal'].isin(cnae_lista)

                # Cria uma máscara para filtrar os CNAEs secundários
                mask_secondary = df['cnae_fiscal_secundaria']\
                                 .apply(lambda x: any(cnae in cnae_lista 
                                                      for cnae in str(x).split(',')))

                # Combina as máscaras e filtra o DataFrame
                df = df[mask_primary | mask_secondary]

                df = df[df['municipio'].isin(cidades) & df['uf'].isin(ufs)]

            # parquet_name = lambda x: parquet_intermediario
            print('Convertendo para parquets intermediários')

            parquet_path = os.path.join(cnpj_dir, 'cnpj_url', f'cnpjs_{ano}_{mes}')
            if not os.path.exists(parquet_path):
                os.makedirs(parquet_path)

            df.write_parquet(file= os.path.join(parquet_path, parquet_mid))
            print("parquet intermediário criado")            

    def unify_parquet(self, ano, mes, cnpj_dir, fonte: str, count = 99):
        parquet_final = os.path.join(cnpj_dir, f'cnpj_{fonte.lower()}_{ano}_{mes}.parquet')

        if os.path.exists(parquet_final):
            return 'Parquet unificado existente, pulando para tratamento'
        
        df = pl.scan_parquet(os.path.join(cnpj_dir, 'cnpj_url', f'cnpjs_{ano}_{mes}')).collect()

        print(df.head())

        try:
            if fonte == 'Estabelecimentos':
                # Converte a coluna 'data_inicio_atividades' para o formato de data
                df = df.with_columns([
                    pl.col('data_inicio_atividades').str.strptime(pl.Date, "%Y%m%d", strict=False).alias('data_inicio_atividades'),
                    pl.col('data_situacao_cadastral').str.strptime(pl.Date, "%Y%m%d", strict=False).alias('data_situacao_cadastral')
                ])

                # Concatena as colunas 'cnpj_basico', 'cnpj_ordem' e 'cnpj_dv' em uma só
                df = df.with_columns(
                    (pl.col('cnpj_basico').cast(pl.Utf8) + pl.col('cnpj_ordem').cast(pl.Utf8) + pl.col('cnpj_dv').cast(pl.Utf8)).alias('cnpj')
                )

                # Renomeia as colunas
                df = df.rename({
                    'matriz_filial': 'identificador_matriz_filial',
                    'data_inicio_atividades': 'data_inicio_atividade',
                    'municipio': 'id_municipio_rf'
                })

                # Cria a coluna 'data' com o valor f'{ano}-{mes}-01'
                df = df.with_columns(
                    pl.lit(f'{ano}-{mes}-01').alias('data')
                )
                
            if fonte == 'Empresas':
                None

            # Salva o DataFrame em um arquivo parquet
            df.write_parquet(parquet_final)

        except FileNotFoundError as e:
            print(f'Processo Finalizado. Arquivos parquet para {ano}-{mes} não encontrado: {e}')

    def limpar_residuo(self, ano: int, mes: str, cnpj_dir: str, fonte: str):
            if fonte == 'Estabelecimentos':
                csv = 'ESTABELE'
            if fonte == 'Empresas':
                csv = 'EMPRECSV'

            try:
                # Path dos arquivos que serão deletados após a conclusão do mês
                csv_to_del = glob(os.path.join(cnpj_dir, fr'*.{csv}'))
                zip_to_del = glob(os.path.join(cnpj_dir, fr'*{ano}_{mes}.zip'))
                tmp_to_del = glob(os.path.join(cnpj_dir, r'*.tmp'))
            except:
                print('não foi possível limpar os arquivos')
                pass

            arquivos = csv_to_del + zip_to_del + tmp_to_del
            list(map(os.remove, arquivos))
    
    def cnpj_treat(self, ano: int, main_dir: str, fonte: str, mes=''):
        file_name = f'cnpj_{fonte.lower()}_{ano}'
        file_name += f'_{mes}.parquet' if mes != '' else '.parquet'
        file_path = os.path.join(main_dir, 'Dados', f'Cnpj {fonte}', file_name)
        string_dtype = pl.String

        try:
            df = pl.read_parquet(file_path)
        except:
            print(f'Não foi possível ler o parquet de {ano}_{mes}')
            return

        # A função foi pensada para ser executada para 'Estabelecimentos' primeiro, depois 'Empresas'
        # Verificar se o arquivo tem mais de 500MB
        if fonte == 'Empresas':
            if os.path.getsize(file_path) > 500 * 1024 * 1024:

                print('entrou no treat de empresas')
                stab_name = f'cnpj_estabelecimentos_{ano}'
                stab_name += f'_{mes}.parquet' if mes != '' else '.parquet'          
                estab_path = os.path.join(main_dir, 'Dados', 'Cnpj Estabelecimentos', stab_name)
                estab_emp_name = f'cnpj_estab_empresas_{ano}'
                estab_emp_name += f'_{mes}.parquet' if mes != '' else '.parquet'

                df_cnpj = pl.read_parquet(estab_path)
                print(f'leu: {estab_path}')

                # Realiza a junção com os dados de estabelecimentos
                cnpj_merge = df_cnpj.join(df, on='cnpj_basico', how='left').lazy()
                cnpj_merge = cnpj_merge.collect()
                print(f'juntou: {estab_path} com {file_path}')
                del df, df_cnpj # removendo df de natureza juridica para evitar dump de RAM
                
                # Salva o resultado da junção no caminho indicado
                cnpj_merge.write_parquet(os.path.join(main_dir, 'Dados', 
                                                      'Cnpj Joined', 
                                                      estab_emp_name))
                print(f'criou: {estab_emp_name}')
                
                print(f"O arquivo {file_path} tem mais de 500MB. Será substituído pelo cnpj_merge.")
                # Remove colunas específicas e substitui o arquivo original por cnpj_merge
                cnpj_merge.drop(['cnpj_ordem', 'cnpj_dv', 'matriz_filial',
                                    'situacao_cadastral', 'data_situacao_cadastral',
                                    'motivo_situacao_cadastral', 'data_inicio_atividades',
                                    'cnae_fiscal', 'cnae_fiscal_secundaria', 'tipo_logradouro',
                                    'logradouro', 'numero', 'complemento', 'bairro',
                                    'cep'])\
                          .write_parquet(file_path)
                print(f'filtrou: {file_path}')
                
                del cnpj_merge
                print(f"Arquivo {file_path} substituído pelo resultado da junção.")
                return
            else:
                print(f"O arquivo {file_path} não foi alterado, pois o tamanho é menor que 500MB.")
                return
        
        print(df.columns)

        # # Remove colunas não necessárias
        if mes == '':
            df = df.drop(['identificador_matriz_filial', 'nome_cidade_exterior',
                          'id_pais', 'id_municipio_nome'])
            df = df.with_columns([
                pl.col('sigla_uf').alias('uf'),
                pl.col('cnae_fiscal_principal').alias('cnae_fiscal')
            ])

            df = df.with_columns([
            pl.col("data").cast(pl.Utf8),
            pl.col("data_inicio_atividade").cast(pl.Utf8),
            pl.col("data_situacao_cadastral").cast(pl.Utf8)
            ])    

            # Trabalha as colunas de data
            df = df.with_columns([
                pl.col("data")
                  .str.strptime(pl.Date, format="%Y-%m-%d").alias("data"),
                pl.col("data_inicio_atividade")
                  .str.strptime(pl.Date, format="%Y-%m-%d").alias("data_inicio_atividade"),
                pl.col("data_situacao_cadastral")
                  .str.strptime(pl.Date, format="%Y-%m-%d").alias("data_situacao_cadastral")
            ])

        elif len(mes) == 2:
            df = df.drop(['cnpj_ordem', 'cnpj_dv', 'matriz_filial',
                        'tipo_logradouro', 'logradouro', 'numero', 'complemento'])
            
            df = df.with_columns([
                pl.col('municipio').alias('id_municipio_rf'),
                pl.col('data_inicio_atividades').alias('data_inicio_atividade')])
            
            df_mun = pl.read_csv(os.path.join(os.getcwd(), 'Dados', 'Ceps Censo', 'ceps_censo.csv'), 
                                 encoding= 'latin1', 
                                 schema={'id_municipio_rf': string_dtype,	'id_municipio_nome': string_dtype,	
                                        'id_municipio': string_dtype, 'sigla_uf': string_dtype})
            
            df = df.join(df_mun, on='id_municipio_rf', how='left')
            
            df = df.with_columns([
            pl.col("data").cast(pl.Utf8),
            pl.col("data_inicio_atividade").cast(pl.Utf8),
            pl.col("data_situacao_cadastral").cast(pl.Utf8)
            ])    

            df = df.with_columns([
                pl.col("data")
                  .str.strptime(pl.Date, format="%Y-%m-%d").alias("data"),
                pl.col("data_inicio_atividade")
                  .str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S.%f").alias("data_inicio_atividade"),
                pl.col("data_situacao_cadastral")
                  .str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S.%f").alias("data_situacao_cadastral")
                ])
            
        df = df.with_columns(pl.col("bairro").map_elements(lambda x: str(normalize_text(x)).title()))

        cep_dict = pl.read_csv(os.path.join(os.getcwd(), 'Dados', 'Ceps Censo', 'ceps_censo.csv'),
                               schema={'cep': string_dtype,	'logradouro': string_dtype,	
                                        'localidade': string_dtype, 'id_municipio': string_dtype, 
                                        'nome_municipio': string_dtype, 'sigla_uf': string_dtype, 
                                        'estabelecimentos': string_dtype, 'centroide': string_dtype})
        
        cep_dict = cep_dict.drop(['logradouro', 'id_municipio', 'sigla_uf', 'estabelecimentos', 'centroide'])\
                           .rename({'localidade': 'bairro', 'nome_municipio': 'id_municipio_nome'})
        
        cep_repo = pl.read_parquet(os.path.join(os.getcwd(), 'Dados', 'Ceps Censo', 'ceps_es.parquet'))
        
        ceps = pl.concat([cep_dict, cep_repo]).unique(subset= ['cep'],keep='last')\
                                              .with_columns(pl.col("bairro")
                                              .map_elements(lambda x: str(normalize_text(x)).title()))
        
        # Realiza o left join entre df e ceps usando 'cep' como chave
        df = df.join(ceps, on='cep', how='left')

        # Substitui a coluna df['bairro'] pela ceps['bairro'] quando disponível
        df = df.with_columns([
            pl.when(pl.col("bairro_right").is_not_null())
            .then(pl.col("bairro_right"))
            .otherwise(pl.col("bairro"))
            .alias("bairro")
        ])

        # Remove a coluna de bairro duplicada (bairro_right)
        df = df.drop("bairro_right")
        # Salva o DataFrame como Parquet
        df.write_parquet(file_path)

    def core_tasks_mt(self, count, url_formatada, cnpj_dir, ano, mes,  file_count,  
                      cidades: list, ufs: list, fonte: str):
        treat = CnpjTreatment()
        process = self

        print(f"Processando {ano}-{mes} (Count: {count})")

        # Baixar e renomear o arquivo
        treat.extrair_cnpj_url(url= url_formatada, cnpj_dir= cnpj_dir, ano= ano, 
                               mes= mes, count= count, fonte= fonte)
        # Descompactar o arquivo
        treat.unzip_cnpj(cnpj_dir= cnpj_dir, ano= ano, mes= mes, count= count, fonte= fonte)
        # Converter para Parquet
        process.shrink_to_parquet(cnpj_dir=cnpj_dir, ano=ano, mes=mes, count= count, 
                                  cidades=cidades, ufs=ufs, fonte= fonte)

    # Função para processamento paralelo download-tratamento dos dados de CNPJ
    def core_tasks_dt(self, downloader: function, processer: function, iterations: int):

        # Função principal que organiza a execução em etapas
        def process_scheduler():
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_downloader = None  # Inicialmente, nenhum download foi executado
                for round in range(0, iterations):  # Três execuções
                    if future_downloader:
                        # Primeiro, esperamos o futuro do downloader da rodada anterior
                        future_downloader.result()
                        
                    # Inicia o próximo download
                    future_downloader = executor.submit(downloader, round)

                    # Inicia o tratamento dos dados da rodada anterior (se houver)
                    if round > 0:
                        future_processer = executor.submit(processer, round-1)
                        future_processer.result()  # Espera o tratamento dos dados da rodada anterior terminar antes de seguir

                # Iniciar o tratamento final dos dados após o último download terminar
                future_downloader.result()  # Aguarda o último download terminar

                executor.submit(processer, round).result()

        process_scheduler()


def one_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, 
               cidades: list, ufs: list, fonte: str):
    treat = CnpjTreatment()
    process = CnpjProcess()

    num_processors = 1

    for count in range(0, file_count):
        print(f"Processando {ano}-{mes} (Count: {count})")

        # Baixar e renomear o arquivo
        treat.extrair_cnpj_url(url=url_formatada, cnpj_dir=cnpj_dir, ano=ano, 
                               mes=mes, count= count, fonte= fonte)
        
        # Descompactar o arquivo
        treat.unzip_cnpj(cnpj_dir=cnpj_dir,  ano=ano, mes=mes, count= count, fonte= fonte)

        # Converter para Parquet
        process.shrink_to_parquet(cnpj_dir=cnpj_dir, ano=ano, mes=mes, count=count, 
                                  cidades=cidades, ufs=ufs, fonte= fonte)


def dual_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, 
                cidades: list, ufs: list, fonte: str):
    treat = CnpjTreatment()
    process = CnpjProcess()
    num_processors = 2        
    print('entrou no routine dual')

    def downloader(curr_count):
        # Baixar e renomear o arquivo
        print(f"Download de {ano}-{mes} (Count: {curr_count})")
        treat.extrair_cnpj_url(url=url_formatada, cnpj_dir=cnpj_dir, ano=ano, mes=mes, 
                               count= curr_count, fonte= fonte)

    def processer(curr_count):
        print(f"Processamento de {ano}-{mes} (Count: {curr_count})")
        # Descompactar o arquivo
        treat.unzip_cnpj(cnpj_dir= cnpj_dir, count= curr_count, ano= ano, mes= mes, 
                         fonte= fonte)
        # Converter para Parquet
        process.shrink_to_parquet(cnpj_dir=cnpj_dir, ano=ano, mes=mes, count= curr_count, 
                                  cidades=cidades, ufs=ufs, fonte= fonte)

    # if __name__ == "__main__":
    process.core_tasks_dt(downloader, processer, file_count)


def multi_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, 
                 cidades: list, ufs: list, fonte: str):
    process = CnpjProcess()

    print('entrou no routine multi')
    
    # Lista dos arquivos a serem baixados (apenas números do arquivo)
    arquivos_para_baixar = [count for count in range(0, file_count)]

    num_processors = int(min(cpu_count() - 4, file_count)/3) # calcula o número de threads que serão utilizadas, arredondando pra baixo

    # Criar e configurar o pool de processos
    with Pool(processes=num_processors) as pool:
        # Em vez de passar o `soup`, passar os dados relevantes para o pool
        tasks = [(count, url_formatada, cnpj_dir, ano, mes, file_count, list(cidades), 
                  list(ufs), fonte) for count in arquivos_para_baixar]
        pool.starmap(process.core_tasks_mt, tasks)  # Envia tarefas para o pool e aguarda a conclusão

def normalize_text(text):
    """Normalizes text by removing diacritics and converting to lowercase."""
    normalized_text = unicodedata.normalize("NFKD", text)
    return normalized_text.encode("ASCII", "ignore").decode("ASCII").lower()