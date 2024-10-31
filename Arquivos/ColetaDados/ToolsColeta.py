"""
ToolsColeta.py
Este arquivo define classes e funções relacionadas à coleta e processamento de dados de CNPJ e à manipulação de arquivos. Abaixo está a documentação detalhada para cada componente do arquivo:

Imports
Bibliotecas Padrão:
pandas as pd
os, zipfile, time, requests, re, sys, wget, urllib
Biblioteca de Análise HTML:
from bs4 import BeautifulSoup
Dataclasses:
from dataclasses import dataclass
Classes e Métodos
1. MainParameters
A classe MainParameters armazena URLs, parâmetros e dados relacionados ao processo de coleta de dados.

Atributos:

url_cnpj: URL base para dados CNPJ.
url_fex, url_fpm, url_fundeb: URLs para arquivos CSV relacionados a transferências.
anos_relatorio: Lista de anos disponíveis para análise.
uf: Lista de Unidades Federativas.
cnae_lista: Lista de códigos CNAE.
cidades_zeec_es: Dicionário que mapeia cidades para códigos IBGE.
cod_ibge_to_rf: Dicionário que mapeia códigos IBGE para códigos RF.
Métodos:

cod_ibge(): Retorna os códigos IBGE das cidades.
nome_mun(): Retorna os nomes das cidades.
cod_rf(): Retorna os códigos RF das cidades.
ufs(): Retorna as Unidades Federativas.
anos_analise(): Retorna os anos disponíveis para análise.
cnae_analise(): Retorna a lista de códigos CNAE.
url_cnpjs(): Retorna a URL base para dados CNPJ.
url_transferencias(where: str): Retorna a URL para os dados de transferências com base no parâmetro where ('fundeb', 'fpm', 'fex').
2. CnpjTreatment
A classe CnpjTreatment é responsável por diversas etapas de tratamento e coleta de dados de CNPJ.

Métodos:

etapa_atual(ano: int, mes: str, cnpj_dir: str, count: int): Verifica a etapa atual do processamento (zip, csv, ou pasta_parquet) para um arquivo específico.
check_url(url: str, cnpj_dir: str, ano: int, mes: str): Verifica a disponibilidade de arquivos na URL e retorna o conteúdo da página e a quantidade de arquivos encontrados.
unzip_cnpj(cnpj_dir: str, count: int, ano: str, mes: str): Descompacta arquivos zip encontrados na pasta especificada.
extrair_cnpj_url(url: str, ano: str, mes: str, count: int, cnpj_dir: str): Baixa e renomeia arquivos zip de CNPJ. Inclui a lógica de retries para o download e renomeia os arquivos baixados.
3. Função save_parquet
Descrição:

Salva um DataFrame em um arquivo Parquet no diretório especificado, renomeando o arquivo temporário para o nome final.
Parâmetros:

main_dir: Diretório principal para salvar o arquivo Parquet.
table_name: Nome da tabela para formar o nome do arquivo.
df: DataFrame a ser salvo.
ano: (Opcional) Ano para formar o sufixo do arquivo.
Passos:

Cria o diretório necessário.
Salva o DataFrame em um arquivo temporário.
Remove o arquivo original se existir.
Renomeia o arquivo temporário para o nome final.
4. Função clean_dots
Descrição:

Limpa e converte valores de string para float, removendo pontos e substituindo vírgulas por pontos decimais.
Parâmetros:

valor: Valor a ser limpo e convertido.
Retorno:

Retorna o valor convertido para float ou None se não for possível a conversão.
"""


from __future__ import annotations
import pandas as pd
import os, zipfile, time, requests, os, re, sys, wget, urllib, gdown
from glob import glob
from bs4 import BeautifulSoup
from dataclasses import dataclass



@dataclass
class MainParameters:

    url_cnpj = 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/{ano}-{mes}/'
    url_fex = r"https://www.tesourotransparente.gov.br/ckan/dataset/3b5a779d-78f5-4602-a6b7-23ece6d60f27/resource/4ca6aad2-fa9d-48e1-a608-5614578d7df2/download/FEX-por-Municipio.csv"
    url_fpm = r"https://www.tesourotransparente.gov.br/ckan/dataset/3b5a779d-78f5-4602-a6b7-23ece6d60f27/resource/d69ff32a-6681-4114-81f0-233bb6b17f58/download/FPM-por-Municipio.csv"
    url_fundeb = r"https://www.tesourotransparente.gov.br/ckan/dataset/3b5a779d-78f5-4602-a6b7-23ece6d60f27/resource/18d5b0ae-8037-461e-8685-3f0d7752a287/download/FUNDEB-por-Municipio.csv"

    anos_relatorio = [2021, 2022, 2023, 2024]
    uf = ["ES"]
    cnae_lista =   ['311601','311602','311603','311604','312401','312403','312404','321301','321302','321303','321304','321305','321399',
                    '322101','322102','322103','322104','322105','322106','322107','322199','600001','600002','600003','810001','810002',
                    '810003','810004','810005','810006','810007','810008','810010','810099','892401','892402','892403','893200','910600',
                    '1020101','1020102','2851800','3011301','3011302','3012100','3314714','3317101','3317102','4221901','4291000','4634603',
                    '4722902','4763604','4763605','5011401','5011402','5012201','5021101','5021102','5022001','5022002','5030101','5030102',
                    '5091201','5091202','5099801','5099899','5231101','5231102','5232000','5239700','5510801','5510802','5590601','5590602',
                    '5590603','5590699','7420002','7490102','7719501','7912100'
                    ]

    cidades_zeec_es =  {'Marataízes': '3203320',
                        'Itapemirim': '3202801',
                        'Cachoeiro de Itapemirim': '3201209',
                        'Presidente Kennedy': '3204302',
                        'Piúma': '3204203',
                        'Anchieta': '3200409',
                        'Guarapari': '3202405',
                        'Viana': '3205101',
                        'Vila Velha': '3205200',
                        'Cariacica': '3201308',
                        'Vitória': '3205309',
                        'Serra': '3205002',
                        'Fundão': '3202207',
                        'Aracruz': '3200607',
                        'Linhares': '3203205',
                        'Sooretama': '3205010',
                        'Jaguaré': '3203056',
                        'São Mateus': '3204906',
                        'Conceição da Barra': '3201605'
                        }
    
    cod_ibge_to_rf =   {'3203320': '760',    # Marataízes
                        '3202801': '5655',   # Itapemirim
                        '3201209': '5623',   # Cachoeiro de Itapemirim
                        '3204302': '5685',   # Presidente Kennedy
                        '3204203': '5683',   # Piúma
                        '3200409': '5607',   # Anchieta
                        '3202405': '5647',   # Guarapari
                        '3205101': '5701',   # Viana
                        '3205200': '5703',   # Vila Velha
                        '3201308': '5625',   # Cariacica
                        '3205309': '5705',   # Vitória
                        '3205002': '5699',   # Serra
                        '3202207': '5643',   # Fundão
                        '3200607': '5611',   # Aracruz
                        '3203205': '5663',   # Linhares
                        '3205010': '766',    # Sooretama
                        '3203056': '5713',   # Jaguaré
                        '3204906': '5697',   # São Mateus
                        '3201605': '5631'    # Conceição da Barra
                        }
    
    # Database Plugin
    def cod_ibge(self: MainParameters):
        cods = self.cidades_zeec_es.values()
        return cods
    
    def nome_mun(self: MainParameters):
        nomes = self.cidades_zeec_es.keys()
        return nomes
    
    def cod_rf(self: MainParameters):
        cods = self.cod_ibge_to_rf.values()
        return cods
    
    def ufs(self: MainParameters):
        ufs = self.uf
        return ufs
    
    def anos_analise(self: MainParameters):
        anos = self.anos_relatorio
        return anos
    
    def cnae_analise(self:MainParameters):
        cnaes = self.cnae_lista
        return cnaes
    
    def url_cnpjs(self:MainParameters):
        url = self.url_cnpj
        return url
    
    def url_transferencias(self:MainParameters, where: str):
        if where == 'fundeb':
            return self.url_fundeb
        
        if where == 'fpm':
            return self.url_fpm
        
        if where == 'fex':
            return self.url_fex



@dataclass
class CnpjTreatment:

    def etapa_atual(self, ano: int, mes: str, cnpj_dir: str, count: int, fonte: str):
        print(cnpj_dir)
        etapa = 'inicial'
        print(os.path.join(cnpj_dir, f'{fonte}{count}_{ano}_{mes}.zip'))
        arquivo_zip = os.path.exists(os.path.join(cnpj_dir, f'{fonte}{count}_{ano}_{mes}.zip'))
        if fonte == 'Estabelecimentos':
            try:
                arquivo_csv = os.path.exists(glob(os.path.join(cnpj_dir, fr'*Y{count}.*.ESTABELE'))[0])
            except:
                arquivo_csv = False
        if fonte == 'Empresas':
            try:
                arquivo_csv = os.path.exists(glob(os.path.join(cnpj_dir, fr'*Y{count}.*.EMPRECSV'))[0])
            except:
                arquivo_csv = False
        
        pasta_parquet = os.path.exists(os.path.join(cnpj_dir, 'cnpj_url', f'cnpjs_{ano}_{mes}', f"cnpj_{ano}_{mes}_{count}"))

        if arquivo_zip:
            etapa = 'zip'
            print(os.path.join(cnpj_dir, f'{fonte}{count}_{ano}_{mes}.zip'))
        if arquivo_csv and fonte == 'Estabelecimentos':
            etapa = 'csv'
            print(glob(os.path.join(cnpj_dir, fr'*Y{count}.*.ESTABELE'))[0])
        if arquivo_csv and fonte == 'Empresas':
            etapa = 'csv'
            print(glob(os.path.join(cnpj_dir, fr'*Y{count}.*.EMPRECSV'))[0])
        if pasta_parquet:
            etapa = 'pasta_parquet'
            print((os.path.join(cnpj_dir, 'cnpj_url', f'cnpjs_{ano}_{mes}', f"cnpj_{ano}_{mes}_{count}")))
        
        return etapa
        
    def check_url(self, url: str, cnpj_dir: str, ano: int, mes: str, fonte: str):
        # requisição da página
        page = requests.get(url.format(ano= ano, mes= mes))   
        data = page.text
        soup = BeautifulSoup(data)
        
        # Pasta de destino para os arquivos zip
        pasta_compactados = cnpj_dir  # Local dos arquivos zipados da Receita

        # Verifica se a pasta existe, se não, cria a pasta
        if not os.path.exists(pasta_compactados):
            os.makedirs(pasta_compactados)

        print('Relação de Arquivos em ' + url)

        # print(soup.find_all('a'))
        
        if soup.find_all('a') == []:
            print(f'''Link para {ano}_{mes} pode não estar disponível, confirme no link http://200.152.38.155/CNPJ/dados_abertos_cnpj. Passando para a próxima etapa''')
            return ''
        
        files_list = [] 
        for link in soup.find_all('a'):
            # Verifica se o link é de um arquivo estabelecimentos\d.zip
            if re.search(fr'{fonte}\d\.zip$', str(link.get('href'))) == None:
                pass
            else:
                files_list.append(re.search(fr'{fonte}\d\.zip$', str(link.get('href'))))
    
        return soup, len(files_list)

    # Função que recebe lê a pasta, procura arquivos zipados e unzipa para a mesma pasta
    def unzip_cnpj(self, cnpj_dir: str, count:int, ano: str, mes: str, fonte: str):

        print(f'Começou a unzip de {ano}-{mes}-{count}')
        zip_name = fr'{fonte}{count}_{ano}_{mes}.zip'

        # Função para verificar se o arquivo csv já existe
        etapa_atual = self.etapa_atual(ano, mes, cnpj_dir, count, fonte)  
        print(etapa_atual)      
        if etapa_atual in ['csv', 'pasta_parquet']:
            return print(f'Arquivo CSV já existente ou processado. \nPulando para conversão para parquet.')

        arq_descompactado = os.path.exists(os.path.join(cnpj_dir, f'cnpjs_{ano}_{mes}'))
        if arq_descompactado:
            print('Pasta com os parquet descompactados já existe')
            return None

        print('Analisando o arquivo' + zip_name)
        zip_path = os.path.join(cnpj_dir, zip_name)
        print('Analisando o csv em:\n' + zip_path)

        print(time.asctime(), 'descompactando ' + zip_path)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            arquivos_no_zip = zip_ref.namelist()
            print('Arquivo(s) csv encontrados no zip: ' + str(arquivos_no_zip))
            zip_ref.extractall(cnpj_dir)

    def extrair_cnpj_url(self, url: str, ano: str, mes: str, count: int, cnpj_dir: str, fonte:str):
        '''http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/{fonte}\d.zip'''

        # Função para verificar se alguma etapa posterior já foi feita
        etapa_atual = self.etapa_atual(ano, mes, cnpj_dir, count, fonte)  
 
        if etapa_atual != 'inicial':
            return print(f'Pulando o download de {ano}_{mes}_{count}.')

        treat = CnpjTreatment()
        soup, file_count = treat.check_url(url=url, cnpj_dir=cnpj_dir, ano=ano, mes=mes, fonte= fonte)
        max_retries = 99
        # Pasta de destino para os arquivos zip
        pasta_compactados = cnpj_dir  # Local dos arquivos zipados da Receita

        def bar_progress(current, total, width=80):
            if total>=2**20:
                tbytes='Megabytes'
                unidade = 2**20
            else:
                tbytes='kbytes'
                unidade = 2**10
            progress_message = f"Download status: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unidade, total//unidade)
            sys.stdout.write("\r" + progress_message)
            sys.stdout.flush()
        
        def get_file_size(url):
            """Função para obter o tamanho do arquivo via Content-Length no cabeçalho HTTP"""
            response = requests.head(url)
            size = response.headers.get('Content-Length')
            if size:
                size = int(size)
                # Converte bytes para MB
                size_in_mb = size / (1024 * 1024)
                return size_in_mb
            return None
        
        print('Começou a baixar o link')
        
        # Verifica se a pasta existe, se não, cria a pasta
        if not os.path.exists(pasta_compactados):
            os.makedirs(pasta_compactados)
        
        # Função para verificar se o arquivo já existe
        def arquivo_existe(arquivo_original, arquivo_novo):
            arq_original = os.path.exists(os.path.join(pasta_compactados, arquivo_original))
            arq_novo = os.path.exists(os.path.join(pasta_compactados, arquivo_novo))
            return arq_original or arq_novo
        
        # Função para verificar se o arquivo zip já existe
        etapa_atual = self.etapa_atual(ano, mes, cnpj_dir, count, fonte)  

        if etapa_atual in ['csv', 'pasta_parquet']:
            return print(f'Arquivo zip já existente ou processado: {cnpj_dir}{fonte}{count}_{ano}_{mes}.zip. \nPulando para extração do zip.')
            
        for link in soup.find_all('a'):
            # Verifica se o link é de um arquivo estabelecimentos\d.zip
            if re.search(fr'{fonte}{count}\.zip$', str(link.get('href'))):
                cam = link.get('href')
                full_url = cam if cam.startswith('http') else url + cam
                
                nome_arquivo_original = os.path.basename(full_url)  # Nome original do arquivo

                nome_arquivo_novo = f'{fonte}{count}_{ano}_{mes}.zip'  # Nome novo do arquivo

                 # Verifica se o arquivo já existe
                if arquivo_existe(nome_arquivo_original, nome_arquivo_novo):
                    print(f"O arquivo {nome_arquivo_novo} já existe. Pulando download.")
                    return nome_arquivo_novo # Sai da função, pois o arquivo já existea
                
                # Obter o tamanho do arquivo
                file_size = get_file_size(full_url)
                
                if file_size:
                    print(f"{full_url} - {file_size:.2f} MB")
                else:
                    print(f"{full_url} - Tamanho: Não disponível")

        # Tentativa de download com possibilidade de retries
        caminho_arquivo_original = os.path.join(pasta_compactados, nome_arquivo_original)
        print('caminho original:' + caminho_arquivo_original)
        for attempt in range(max_retries):
            try:
                print(f'\n{time.asctime()} - Iniciando download do item {count}: {full_url}')
                wget.download(full_url, out=caminho_arquivo_original, bar=bar_progress)

                # Renomeia o arquivo baixado para o novo nome
                caminho_arquivo_novo = os.path.join(pasta_compactados, nome_arquivo_novo)
                print('caminho novo:' + caminho_arquivo_novo)
                os.rename(caminho_arquivo_original, caminho_arquivo_novo)
                print(f"\nArquivo baixado e renomeado para {caminho_arquivo_novo}")
                return None

            except urllib.error.ContentTooShortError as e:
                print(f"\nErro ao baixar o arquivo (tentativa {attempt + 1}/{max_retries}): {e}")
                print("Tentando novamente...")
                time.sleep(3)  # Pequena pausa antes de tentar novamente

            except OSError as e:
                print(f"\nErro ao renomear o arquivo {caminho_arquivo_original}: {e}")


def save_parquet(main_dir: str, table_name: str, df: pd.DataFrame, ano: int = None):

    # Garante que o diretório existe
    dir_path = os.path.join(main_dir, "Dados", " ".join([word.capitalize() for word in table_name.split(sep="_")]))
    os.makedirs(dir_path, exist_ok=True)
    
    # Define o caminho completo do arquivo Parquet e o temporário
    file_suffix = f"_{ano}" if ano else ""
    file_path = os.path.join(dir_path, f'{table_name}{file_suffix}.parquet')
    temp_file_path = os.path.join(dir_path, f'{table_name}{file_suffix}_temp.parquet')

    # Salva o DataFrame no arquivo temporário
    df.to_parquet(temp_file_path, index=False)
    print(f"Arquivo temporário salvo em: {temp_file_path}")
    
    # Verifica se o arquivo original já existe e o remove
    if os.path.exists(file_path):
        print(f"Arquivo original encontrado. Removendo: {file_path}")
        os.remove(file_path)
    
    # Renomeia o arquivo temporário para o nome final
    os.rename(temp_file_path, file_path)
    print(f"Arquivo final salvo em: {file_path}")


# Função para limpar a formatação de separadores de milhar e decimais
def clean_dots(value):
    print(f"Valor original: {value}")  # Imprime o valor original
    try:
        # Adicione a lógica aqui para remover pontos e converter para número
        cleaned_value = value.replace('.', '').replace(',', '.')
        cleaned_value = float(cleaned_value)
        print(f"Valor após limpeza: {cleaned_value}")  # Imprime o valor limpo
        return cleaned_value
    except Exception as e:
        print(f"Erro ao limpar o valor: {value}, Erro: {e}")
        return None  # Retorna None se ocorrer erro na conversão

def bar_progress(current, total, width=80):
    """Exibe a barra de progresso para acompanhar a leitura dos arquivos."""
    progress = current / total
    progress_message = f"Baixando arquivos: {int(progress * 100)}% [{current} / {total}]"
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

def download_overwrite(dw_path: str, file_name: str, url: str):
    file_path = os.path.join(dw_path, file_name)
    old_file = os.path.join(dw_path, 'old_' + file_name)    
    # Verifica se o arquivo já existe antes de renomear
    if os.path.exists(file_path):
        os.rename(file_path, old_file)
    try:
        # Faz o download do novo arquivo
        wget.download(url=url, out=file_path, bar= bar_progress)
        # Se o download foi bem-sucedido, remove o arquivo antigo
        if os.path.exists(old_file):
            os.remove(old_file)
    except:
        try:
            gdown.download(url, file_path, quiet=False)
        except Exception as e:
            # Caso ocorra um erro, restaura o arquivo antigo
            print(f"Erro ao baixar o arquivo: {e}")
            if os.path.exists(old_file):
                os.rename(old_file, file_path)
