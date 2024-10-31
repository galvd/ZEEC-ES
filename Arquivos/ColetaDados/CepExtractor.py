from __future__ import annotations
import sys, json, os, zipfile, shutil, requests, re
import polars as pl
import pandas as pd
from time import sleep

with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])


from Arquivos.ColetaDados.Extrator import extrair_dados_sql
from Arquivos.ColetaDados.ToolsColeta import bar_progress


def extrair_ceps(main_dir: str = None, ufs: str = [], limit: str = ""):
    ufs_sql = ", ".join(f"{uf}" for uf in ufs)

    # Query gerada pelo site da Base dos Dados: https://basedosdados.org/dataset/33b49786-fb5f-496f-bb7c-9811c985af8e?table=566ed7f2-db5f-4ac3-bfce-b579137c993c
    query_cep = """
        SELECT
            dados.cep as cep,
            dados.logradouro as logradouro,
            dados.localidade as localidade,
            dados.id_municipio as id_municipio,
            dados.nome_municipio as nome_municipio,
            dados.sigla_uf as sigla_uf,
            dados.estabelecimentos as estabelecimentos,
            dados.centroide as centroide
        FROM `basedosdados.br_bd_diretorios_brasil.cep` AS dados


        WHERE 
            sigla_uf in {ufs_sql} 
                             
            """


    processamento_cep = extrair_dados_sql(
    table_name= "ceps_censo",
    query_base=query_cep,
    main_dir=main_dir,
    ufs=ufs,
    limit=limit
    )


# Função que coleta os CEPs do Brasil baseados na v1. Não contém todos os CEPs.
def extrair_ceps_v1_es(main_dir:str):
    url = r'https://github.com/SeuAliado/OpenCEP/releases/download/2.0.1/v1.zip'
    input_folder =  os.path.join(main_dir, 'Dados', 'Ceps Censo')
    zip_file_path = os.path.join(input_folder, 'v1.zip')

    # Verificar se o diretório de entrada existe, caso contrário, criá-lo
    if not os.path.exists(input_folder):
        os.makedirs(input_folder)
        print(f"Pasta {input_folder} criada.")

    # Caminho da pasta extraída e renomeada
    renamed_folder = os.path.join(input_folder, 'ceps_brasil')

    # Verificar se a pasta 'ceps_brasil' já contém mais de 1.200.000 arquivos
    if os.path.exists(renamed_folder):
        num_files = len([f for f in os.listdir(renamed_folder) if f.endswith(".json")])
        if num_files > 1200000:
            print(f"A pasta 'ceps_brasil' já contém {num_files} arquivos. Extração desnecessária.")
        else:
            print(f"Pasta 'ceps_brasil' encontrada, mas com menos de 1.200.000 arquivos ({num_files}). Continuando extração.")
    else:
        # Baixar o arquivo zip
        print(f"Baixando o arquivo zip de {url}...")
        response = requests.get(url)
        with open(zip_file_path, 'wb') as f:
            f.write(response.content)
        print("Download concluído.")

        # Extrair o conteúdo da pasta 'v1'
        print("Extraindo o conteúdo do arquivo zip...")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(input_folder)
        
        # Caminho original da pasta extraída
        extracted_folder = os.path.join(input_folder, 'v1')

        # Renomear a pasta 'v1' para 'ceps_brasil'
        if os.path.exists(extracted_folder):
            shutil.move(extracted_folder, renamed_folder)
            print(f"Pasta 'v1' renomeada para 'ceps_brasil' em {renamed_folder}")
        else:
            print("A pasta 'v1' não foi encontrada após a extração.")
            return
        
        # Remover o arquivo zip após a extração
        os.remove(zip_file_path)
        print("Arquivo zip removido após a extração.")

    # Lista completa de prefixos de CEP do Espírito Santo; Alterar para outros prefixos dos estados desejados
    es_prefixes = ["29"]
    
    # Lista para armazenar os dados
    data = []

    # Contar quantos arquivos existem no diretório
    all_files = [f for f in os.listdir(renamed_folder) if f.endswith(".json")]
    total_files = len(all_files)

    if total_files == 0:
        print("Nenhum arquivo JSON encontrado no diretório.")
        return
    
    # Iterar sobre os arquivos JSON na pasta
    for i, filename in enumerate(all_files, start=1):
        # Atualizar a barra de progresso
        bar_progress(i, total_files)

        # Verificar se o arquivo tem um prefixo válido para ES
        if filename.endswith(".json") and any(filename.startswith(prefix) for prefix in es_prefixes):
            filepath = os.path.join(renamed_folder ,filename)
            try:
                # Abrir e ler o conteúdo do arquivo JSON
                with open(filepath, "r", encoding="utf-8") as file:
                    cep_data = json.load(file)
                    
                    # Verificar se as chaves necessárias estão presentes
                    if all(key in cep_data for key in ["cep", "bairro", "localidade", "uf"]) and cep_data["uf"] == "ES":
                        # Armazenar os dados
                        data.append({
                            "cep": cep_data["cep"].replace("-", ""),  # Remover o hífen do CEP
                            "bairro": cep_data["bairro"],
                            "localidade": cep_data["localidade"]
                        })
            except json.JSONDecodeError:
                print(f"Erro ao ler o arquivo {filepath}")
                continue

    # Verificar se a lista de dados está vazia
    if data:
        # Criar DataFrame a partir dos dados
        df = pl.DataFrame(data)
        # Verificar as colunas e dados antes de continuar
        print("Colunas do DataFrame:", df.columns)
        print("Dados do DataFrame (primeiras 5 linhas):")
        print(df.head())

        # Tratamento das colunas
        df = df.with_columns([pl.col("localidade").alias("id_municipio_nome"),
                              pl.when((pl.col("bairro") == "") | (pl.col("bairro").is_null()))
                                .then(pl.lit("Sem Bairro/Não Identificado"))
                                .otherwise(pl.col("bairro"))
                                .alias("bairro")
        ]).drop('localidade')
               
        # Salvar em um arquivo Parquet
        df.write_parquet(os.path.join(input_folder, 'ceps_es.parquet'))
        print(f"\nArquivo salvo com {len(df)} CEPs do ES.")
    else:
        print("\nNenhum CEP válido do ES foi encontrado.")


# Função para extrair os CEPs restantes de acordo com a base v2. Coleta os CEPs linha a linha
def extrair_ceps_v2_es():

    # Carregar o arquivo Excel que contém os CEPs faltantes
    missing_ceps = pd.read_excel(r'C:\Users\galve\MyDrive\Carreira\ObservatorioES\Dados\cep_merge.xlsx')

    # Função para remover caracteres não numéricos do CEP
    def limpar_cep(cep):
        return re.sub(r'\D', '', cep)

    # Lista para armazenar os dados extraídos
    dados_cep = []

    # Contadores para visualização do progresso
    total_ceps = len(missing_ceps)
    sucessos = 0
    falhas = 0

    # Iterar sobre cada CEP no dataframe
    for index, cep in enumerate(missing_ceps['cep']):
        try:
            if str(cep).startswith("29"):
                pass
            else:
                continue
            # Limpar o CEP para garantir que seja numérico
            cep_limpo = limpar_cep(str(cep))
            
            # Montar a URL da API
            url = f"https://brasilapi.com.br/api/cep/v2/{cep_limpo}"
            
            # Fazer a requisição à API
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                # Verificar se o campo 'bairro' e 'localidade' existem no JSON
                bairro = data.get('bairro', 'Não encontrado')
                localidade = data.get('localidade', 'Não encontrada')
                dados_cep.append({'cep': cep_limpo, 'bairro': bairro, 'localidade': localidade})
                
                sucessos += 1
                print(f"[{index+1}/{total_ceps}] Sucesso - CEP: {cep_limpo}, Bairro: {bairro}, Cidade: {localidade}")
            else:
                # Adicionar erro na lista
                dados_cep.append({'cep': cep_limpo, 'bairro': 'Erro', 'localidade': 'Erro'})
                falhas += 1
                print(f"[{index+1}/{total_ceps}] Falha - CEP: {cep_limpo}, Status: {response.status_code}")

            sleep(2)

        except:
            print('Requisições com erro. Salvando o progresso.')
            df_ceps_bairros = pd.DataFrame(dados_cep)


    # Exibir o total de sucessos e falhas
    print(f"Total de requisições bem-sucedidas: {sucessos}")
    print(f"Total de requisições com falha: {falhas}")
        

    # Criar o DataFrame final com as informações de CEP e Bairro
    df_ceps_bairros = pd.DataFrame(dados_cep)

    # Exibir o DataFrame resultante
    print(df_ceps_bairros)

    df_ceps_bairros.to_excel(r'C:\Users\galve\MyDrive\Carreira\ObservatorioES\Dados\cep_api.xlsx', index=False)