# -*- coding: utf-8 -*-
"""
Spyder Editor

lista relação de arquivos na página de dados públicos da receita federal
e faz o download
"""
from bs4 import BeautifulSoup
import requests, wget, os, sys, time, glob, re


url = 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/2024-08/'


pasta_compactados = os.getcwd()  + r"Dados\\cnpj_receita" #local dos arquivos zipados da Receita

if len(glob.glob(os.path.join(pasta_compactados,'*.zip'))):
    print(f'Há arquivos zip na pasta {pasta_compactados}. Apague ou mova esses arquivos zip e tente novamente')
    sys.exit()
       
page = requests.get(url)   
data = page.text
soup = BeautifulSoup(data)

lista = []
tamanho_total = 0  # Variável para armazenar o tamanho total dos arquivos
print('Relação de Arquivos em ' + url)

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

for link in soup.find_all('a'):
    if re.search(r'stabelecimentos\d\.zip$', str(link.get('href'))):
        cam = link.get('href')
        if not cam.startswith('http'):
            full_url = url + cam
        else:
            full_url = cam
        
        # Obter o tamanho do arquivo
        file_size = get_file_size(full_url)
        
        if file_size:
            print(f"{full_url} - {file_size:.2f} MB")
            tamanho_total += file_size  # Adiciona o tamanho à soma total
        else:
            print(f"{full_url} - Tamanho: Não disponível")
        
        lista.append(full_url)

# Exibe o tamanho total esperado dos arquivos
print(f"\nTamanho total esperado dos arquivos: {tamanho_total:.2f} MB")
            
resp = input(f'Deseja baixar os arquivos acima para a pasta {pasta_compactados} (y/n)?')
if resp.lower()!='y' and resp.lower()!='s':
    sys.exit()
    
def bar_progress(current, total, width=80):
    if total>=2**20:
        tbytes='Megabytes'
        unidade = 2**20
    else:
        tbytes='kbytes'
        unidade = 2**10
    progress_message = f"Baixando: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unidade, total//unidade)
    # Don't use print() as it will print in new line every time.
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()
  
for k, url in enumerate(lista):
    print('\n' + time.asctime() + f' - item {k}: ' + url)
    wget.download(url, out=os.path.join(pasta_compactados, os.path.split(url)[1]), bar=bar_progress)
    
print('\n\n'+ time.asctime() + f' Finalizou!!! Baixou {len(lista)} arquivos.')

#lista dos arquivos
'''
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos0.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos1.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos2.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos3.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos4.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos5.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos6.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos7.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos8.zip
http://200.152.38.155/CNPJ/dados_abertos_cnpj/AAAA-mm/Estabelecimentos9.zip
'''