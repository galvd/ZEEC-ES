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
import os, concurrent.futures, sys, json
import dask.dataframe as dd
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
    def shrink_to_parquet(self, cnpj_dir: str, ano: str, mes: str, count: int, cidades: list, ufs:list,):
        param = MainParameters()
        cnae_lista = param.cnae_analise()
        parquet_intermediario = f"cnpj_{ano}_{mes}_{count}"

        print('Iniciando conversão e tratamento de csv para parquet')

        # Função para verificar se o csv já foi transformado em parquets
        etapa_atual = self.treat.etapa_atual(ano, mes, cnpj_dir, count)
        print(etapa_atual)
        if etapa_atual == 'pasta_parquet':
            return print(f'Pasta de parquets já existente: {parquet_intermediario} \nPulando para unificação dos parquets.')

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
        
        arq_descompactados = glob(os.path.join(cnpj_dir, fr'*Y{count}.*.ESTABELE'))

        print('Iniciando leitura dos csv para conversão')
        for arq in arq_descompactados:
            print(f'Lendo o csv n{count}' + arq)
            ddf = dd.read_csv(arq, sep=';', header=None, names=colunas_estabelecimento, encoding='latin1', dtype=str, na_filter=None)
            ddf = ddf[['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
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
            
            print('csv lido e colunas fora do escopo retiradas')

            # Cria uma máscara para filtrar os CNAEs primários
            mask_primary = ddf['cnae_fiscal'].isin(cnae_lista)

            # Cria uma máscara para filtrar os CNAEs secundários
            mask_secondary = ddf['cnae_fiscal_secundaria'].apply(lambda x: any(cnae in cnae_lista for cnae in str(x).split(',')))

            # Combina as máscaras e filtra o DataFrame
            ddf = ddf[mask_primary | mask_secondary]

            ddf = ddf[ddf['municipio'].isin(cidades) & ddf['uf'].isin(ufs)]

            # parquet_name = lambda x: parquet_intermediario
            ddf.to_parquet(path = os.path.join(cnpj_dir, 'cnpj_url', f'cnpjs_{ano}_{mes}', parquet_intermediario))
            
    def unify_parquet(self, ano, mes, cnpj_dir, count = 99):
        parquet_final = os.path.join(cnpj_dir, f'cnpjs_{ano}_{mes}.parquet')

        try:
            df = pd.read_parquet(path = os.path.join(cnpj_dir, 'cnpj_url', f'cnpjs_{ano}_{mes}'))
            print(df.head())
            
            print(parquet_final)
            df.to_parquet(parquet_final)

        except FileNotFoundError as e:
            print(f'Processo Finalizado. Arquivos parquet para {ano}-{mes} não encontrado: {e}')

    def limpar_residuo(self, ano, mes, cnpj_dir):
            try:
                # Path dos arquivos que serão deletados após a conclusão do mês
                csv_to_del = glob(os.path.join(cnpj_dir, r'*.ESTABELE'))
                zip_to_del = glob(os.path.join(cnpj_dir, fr'*{ano}_{mes}.zip'))
                tmp_to_del = glob(os.path.join(cnpj_dir, r'*.tmp'))
            except:
                pass

            arquivos = csv_to_del + zip_to_del + tmp_to_del
            list(map(os.remove, arquivos))

    def core_tasks_mt(self, count, url_formatada, cnpj_dir, ano, mes,  file_count,  cidades: list, ufs: list):
        treat = CnpjTreatment()
        process = self

        print(f"Processando {ano}-{mes} (Count: {count})")

        # Baixar e renomear o arquivo
        treat.extrair_cnpj_url(url= url_formatada, cnpj_dir= cnpj_dir, ano= ano, mes= mes, count= count)
        # Descompactar o arquivo
        treat.unzip_cnpj(cnpj_dir= cnpj_dir, ano= ano, mes= mes, count= count)
        # Converter para Parquet
        process.shrink_to_parquet(cnpj_dir=cnpj_dir, ano=ano, mes=mes, count= count, cidades=cidades, ufs=ufs)

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


def one_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, cidades: list, ufs: list):
    treat = CnpjTreatment()
    process = CnpjProcess()

    num_processors = 1

    for count in range(0, file_count):
        print(f"Processando {ano}-{mes} (Count: {count})")

        # Baixar e renomear o arquivo
        treat.extrair_cnpj_url(url=url_formatada, cnpj_dir=cnpj_dir, ano=ano, mes=mes, count= count)
        
        # Descompactar o arquivo
        treat.unzip_cnpj(cnpj_dir=cnpj_dir,  ano=ano, mes=mes, count= count)

        # Converter para Parquet
        process.shrink_to_parquet(cnpj_dir=cnpj_dir, ano=ano, mes=mes, count=count, cidades=cidades, ufs=ufs)


def dual_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, cidades: list, ufs: list):
    treat = CnpjTreatment()
    process = CnpjProcess()
    num_processors = 2        
    print('entrou no routine dual')

    def downloader(curr_count):
        # Baixar e renomear o arquivo
        print(f"Download de {ano}-{mes} (Count: {curr_count})")
        treat.extrair_cnpj_url(url=url_formatada, cnpj_dir=cnpj_dir, ano=ano, mes=mes, count= curr_count)

    def processer(curr_count):
        print(f"Processamento de {ano}-{mes} (Count: {curr_count})")
        # Descompactar o arquivo
        treat.unzip_cnpj(cnpj_dir= cnpj_dir, count= curr_count, ano= ano, mes= mes)
        # Converter para Parquet
        process.shrink_to_parquet(cnpj_dir=cnpj_dir, ano=ano, mes=mes, count= curr_count, cidades=cidades, ufs=ufs)

    # if __name__ == "__main__":
    process.core_tasks_dt(downloader, processer, file_count)





def multi_thread(cnpj_dir: str, ano: int, mes: str, url_formatada: str, file_count: int, cidades: list, ufs: list):
    process = CnpjProcess()

    num_processors = int(min(cpu_count() - 4, 10)/3) # calcula o número de threads que serão utilizadas, arredondando pra baixo

    print('entro no routine multi')
    
    # Lista dos arquivos a serem baixados (apenas números do arquivo)
    arquivos_para_baixar = [count for count in range(0, file_count)]

    num_processors = int(min(cpu_count() - 4, file_count)/3) # calcula o número de threads que serão utilizadas, arredondando pra baixo

    # Criar e configurar o pool de processos
    with Pool(processes=num_processors) as pool:
        # Em vez de passar o `soup`, passar os dados relevantes para o pool
        tasks = [(count, url_formatada, cnpj_dir, ano, mes) for count in arquivos_para_baixar]
        pool.starmap(process.core_tasks_mt, tasks)  # Envia tarefas para o pool e aguarda a conclusão

