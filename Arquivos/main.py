"""
main.py

Este arquivo serve como ponto de entrada para o processamento e extração de dados. Ele configura os parâmetros, carrega as configurações e executa funções para extrair e processar dados de diferentes fontes.

Imports
Bibliotecas Padrão:

json
sys
os
datetime
Módulos Locais:

from Arquivos.ColetaDados.ToolsColeta import MainParameters
from Arquivos.ColetaDados.ExtratorRais import extrair_rais
from Arquivos.ColetaDados.ExtratorCaged import extrair_caged
from Arquivos.ColetaDados.ExtratorEnem import extrair_enem
from Arquivos.ColetaDados.ExtratorEdubase import extrair_edu_base
from Arquivos.ColetaDados.ExtratorPib import extrair_pib_cidades
from Arquivos.ColetaDados.ExtratorIes import extrair_ies, extrair_cursos_sup
from Arquivos.ColetaDados.ExtratorCNPJ import extrair_empresas_bd, extrair_empresas_url, extrair_munic_dict
from Arquivos.ColetaDados.ExtratorConectividade import extrair_internet_acs, extrair_internet_dens
from Arquivos.ColetaDados.ExtratorCenso import extrair_censo_agua, extrair_censo_esgoto, extrair_censo_pop, extrair_censo_alfabetizados
from Arquivos.ColetaDados.ExtratorTransferencias import extrair_transferencias_fex, extrair_transferencias_fpm, extrair_transferencias_fundeb
Descrição do Processo
Configuração e Inicialização

Carrega o arquivo de configuração config.json para obter o caminho da rede e ajustar o sys.path.
Obtém o diretório de trabalho atual (proj_dir).
Cria uma instância da classe MainParameters para acessar os parâmetros de análise.
Definição de Parâmetros

Define os parâmetros como anos de relatório, unidades federativas, códigos IBGE, nomes de municípios, códigos RF e URLs para dados CNPJ.
Define um limite opcional (limit) para testes.
Extração de Dados

RAIS: Extrai dados da RAIS utilizando a função extrair_rais.
CAGED: Extrai dados do CAGED utilizando a função extrair_caged.
Censo: Extrai dados do Censo em diversas categorias utilizando funções específicas (extrair_censo_pop, extrair_censo_agua, etc.).
PIB Municípios: Extrai dados do PIB das cidades utilizando a função extrair_pib_cidades.
Acesso à Internet Banda Larga: Extrai dados sobre acesso à internet em cidades utilizando extrair_internet_acs e extrair_internet_dens.
Ensino Superior: Extrai dados sobre cursos superiores e instituições de ensino utilizando extrair_cursos_sup e extrair_ies.
Educação Básica: Extrai dados sobre educação básica utilizando extrair_edu_base.
ENEM: Extrai dados do ENEM utilizando a função extrair_enem.
Transferências Municipais: Extrai dados sobre transferências municipais utilizando extrair_transferencias_fex, extrair_transferencias_fundeb e extrair_transferencias_fpm.
CNPJs:
Extrai dados de empresas do banco de dados utilizando a função extrair_empresas_bd.
Habilita o processamento paralelo para o download e tratamento dos dados de CNPJ com extrair_empresas_url.
Dicionário de Municípios

Comentado: Há uma chamada para a função extrair_empresas_dict que está comentada, possivelmente para uma tarefa futura relacionada à extração de dicionários de municípios.
Finalização

Exibe uma mensagem indicando que o processamento dos dados está completo.
Calcula e exibe o tempo total de execução do script.
Observações
O script utiliza processamento paralelo para melhorar a eficiência na extração e processamento de dados de CNPJ.
Certifique-se de que o arquivo config.json está corretamente configurado e acessível, pois é essencial para definir o caminho da rede.
O limite opcional (limit) está definido como "LIMIT 10" para fins de teste; ajuste conforme necessário.

"""



from __future__ import annotations
import json, sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])
from os import getcwd
from datetime import datetime

from Arquivos.ColetaDados.ToolsColeta import MainParameters
from Arquivos.ColetaDados.ExtratorRais import extrair_rais
from Arquivos.ColetaDados.ExtratorCaged import extrair_caged
from Arquivos.ColetaDados.ExtratorEnem import extrair_enem
from Arquivos.ColetaDados.ExtratorEdubase import extrair_edu_base
from Arquivos.ColetaDados.ExtratorPib import extrair_pib_cidades
from Arquivos.ColetaDados.ExtratorIes import extrair_ies, extrair_cursos_sup
from Arquivos.ColetaDados.ExtratorCNPJ import extrair_empresas_bd, extrair_empresas_url, extrair_munic_dict
from Arquivos.ColetaDados.ExtratorConectividade import extrair_internet_acs, extrair_internet_dens
from Arquivos.ColetaDados.ExtratorCenso import extrair_censo_agua, extrair_censo_esgoto, extrair_censo_pop, extrair_censo_alfabetizados
from Arquivos.ColetaDados.ExtratorTransferencias import extrair_transferencias_fex, extrair_transferencias_fpm, extrair_transferencias_fundeb


beg = datetime.now()


proj_dir = getcwd()
parametros = MainParameters()
anos_relatorio = parametros.anos_analise()
uf = parametros.ufs()
cod_ibge = parametros.cod_ibge()
nome_mun = parametros.nome_mun()
cod_rf = parametros.cod_rf()
url_cnpj = parametros.url_cnpjs()


limit = "LIMIT 10" # argumento opcional para teste


### Extração dos dados

## RAIS

extrair_rais(
    anos = anos_relatorio,
    cidades = nome_mun,
    main_dir = proj_dir,
    ufs = uf,
    limit = limit
    )


## CAGED

extrair_caged(
    anos = anos_relatorio,
    cidades = nome_mun,
    main_dir = proj_dir,
    ufs = uf,
    mes = 12,
    limit = limit
    )


## Censo

extrair_censo_pop(
    cidades = nome_mun,
    main_dir = proj_dir,
    limit = limit
    )

extrair_censo_agua(
    cidades = nome_mun,
    main_dir = proj_dir,
    limit = limit
    )

extrair_censo_esgoto(
    cidades = nome_mun,
    main_dir = proj_dir,
    limit = limit
    )

extrair_censo_alfabetizados(
    cidades = nome_mun,
    main_dir = proj_dir,
    limit = limit
    )


# PIB Municípios

extrair_pib_cidades(
    anos = anos_relatorio,
    cidades = nome_mun,
    main_dir = proj_dir,
    limit = limit
    )


## Acesso à Internet Banda Larga

extrair_internet_acs(
    anos = anos_relatorio,
    cidades = nome_mun,
    main_dir = proj_dir,
    ufs = uf,
    mes = 12,
    limit = limit
    )

extrair_internet_dens(
    anos = anos_relatorio,
    cidades = nome_mun,
    main_dir = proj_dir,
    ufs = uf,
    mes = 12,
    limit = limit
    )


## Ensino Superior
extrair_cursos_sup(
    anos = anos_relatorio,
    cidades = nome_mun,
    main_dir = proj_dir,
    ufs = uf,
    limit = limit
    )

extrair_ies(
    anos = anos_relatorio,
    cidades = nome_mun,
    main_dir = proj_dir,
    ufs = uf,
    limit = limit
    )


## Educação Básica
# # municípios indexados pelo código ibge
extrair_edu_base(
    anos = anos_relatorio,
    cidades = cod_ibge,
    main_dir = proj_dir,
    ufs = uf,
    limit = limit
    )


## ENEM
# municípios indexados pelo código ibge
extrair_enem(
    anos = anos_relatorio,
    cidades = cod_ibge,
    main_dir = proj_dir,
    ufs = uf,
    limit = limit
    )

# Transferëncias Municipais

extrair_transferencias_fex(cidades= nome_mun, 
                           main_dir = proj_dir,
                           ufs = uf)

extrair_transferencias_fundeb(cidades= nome_mun, 
                           main_dir = proj_dir,
                           ufs = uf)

extrair_transferencias_fpm(cidades= nome_mun, 
                           main_dir = proj_dir,
                           ufs = uf)


## CNPJs
extrair_empresas_bd(
    anos = anos_relatorio,
    cidades = cod_ibge,
    main_dir = proj_dir,
    ufs = uf,
    limit = limit
    )

# habilitando o processamento paralelo para download e tratamento dos dados de CNPJ
if __name__ == '__main__':
    extrair_empresas_url(anos = anos_relatorio,
        cidades = cod_rf,
        main_dir = proj_dir,
        processadores = 'um',
        ufs = uf)

## Dicionário de municípios com código IBGE e Código RF
# extrair_empresas_dict(
#     main_dir = proj_dir
#     )


print("Processamento dos dados completo")

end_time = datetime.now()
time_length = end_time - beg
print(f'Finished at {end_time}.\nTime elapsed: {time_length}.')


