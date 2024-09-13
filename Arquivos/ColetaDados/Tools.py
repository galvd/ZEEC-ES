from __future__ import annotations
import pandas as pd
import os, zipfile, time
import dask.dataframe as dd
from glob import glob
from dataclasses import dataclass



def save_parquet(save_dir: str, table_name: str, df: pd.DataFrame, ano: int = None):

    # Garante que o diretório existe
    dir_path = os.path.join(save_dir, "Dados", " ".join([word.capitalize() for word in table_name.split(sep="_")]))
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
def clean_dots(valor):
    if isinstance(valor, str):
        # Remover os pontos como separadores de milhar e substituir os espaços por ponto decimal
        valor_limpo = valor.replace('.', '').replace(',', '.').strip()
        try:
            return float(valor_limpo)
        except ValueError:
            return None
    return valor

@dataclass
class MainParameters:

    anos_relatorio = [2021, 2022]
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
    

@dataclass
class CnpjTreatment:


    # Função que recebe lê a pasta, procura arquivos zipados e unzipa para a mesma pasta
    def unzip_cnpj(self, dir: str, zip_name:str, ano: str, mes: str):
        if zip_name != '':
            arq_descompactado = os.path.exists(os.path.join(dir, f'cnpjs_{ano}_{mes}'))
            if arq_descompactado:
                print('Pasta com os parquet descompactados já existe')
                return None

            print('Analisando o arquivo' + zip_name)
            zip_path = os.path.join(dir, zip_name)
            print('Analisando o csv em:\n' + zip_path)
    
            print(time.asctime(), 'descompactando ' + zip_path)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                arquivos_no_zip = zip_ref.namelist()
                print('Arquivo(s) csv encontrados no zip: ' + str(arquivos_no_zip))
                zip_ref.extractall(dir)
        if zip_name == '':
            print('Não foi possível obter o nome dos arquivos zip. Verifique se o arquivo foi baixado corretamente.')

    # Função que lê a pasta onde os arquivos foram descompactados por unzip_cnpj, coleta apenas as colunas e linhas de interesse e salva a tabela tratada em .parquet
    def shrink_to_parquet(self, dir: str, ano: str, mes: str):
        print('Iniciando conversão e tratamento de csv para parquet')

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
        
        arq_descompactados = glob(os.path.join(dir, r'*.ESTABELE'))

        print('Iniciando leitura dos csv para conversão')
        count = 0
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
            
            p = MainParameters

            cnae_lista = p.cnae_analise(p)

            # Cria uma máscara para filtrar os CNAEs primários
            mask_primary = ddf['cnae_fiscal'].isin(cnae_lista)

            # Cria uma máscara para filtrar os CNAEs secundários
            mask_secondary = ddf['cnae_fiscal_secundaria'].apply(lambda x: any(cnae in cnae_lista for cnae in str(x).split(',')))

            # Combina as máscaras e filtra o DataFrame
            ddf = ddf[mask_primary | mask_secondary]

            ddf = ddf[ddf['municipio'].isin(p.cod_rf(p)) & ddf['uf'].isin(p.uf)]

            parquet_name = lambda x: f"cnpj_{ano}_{mes}__{count}_{x}.parquet"
            ddf.to_parquet(path = dir + f'\\cnpj_url\\cnpjs_{ano}_{mes}', name_function=parquet_name)
            count += 1

        try:
            df = pd.read_parquet(path = dir + f'\\cnpj_url\\cnpjs_{ano}_{mes}')
            print(df.head())
            
            df.to_parquet(dir + f'\\cnpjs_{ano}_{mes}.parquet')

        except FileNotFoundError as e:
            print(f'Processo Finalizado. Arquivos parquet para {ano}-{mes} não encontrado: {e}')

            


            
