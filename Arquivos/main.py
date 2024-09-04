from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])
from os import getcwd, chdir

from Arquivos.ColetaDados.ExtratorRais import extrair_rais
from Arquivos.ColetaDados.ExtratorCaged import extrair_caged
from Arquivos.ColetaDados.ExtratorEnem import extrair_enem
from Arquivos.ColetaDados.ExtratorCenso import extrair_censo_agua, extrair_censo_esgoto, extrair_censo_pop, extrair_censo_alfabetizados


proj_dir = getcwd()
anos_relatorio = [2021, 2022]
uf = ["ES"]
cidades_zeec = ['Marataízes', 'Itapemirim', 'Cachoeiro de Itapemirim', 
            'Presidente Kennedy', 'Piúma', 'Anchieta', 'Guarapari', 
            'Viana', 'Vila Velha', 'Cariacica', 'Vitória', 
            'Serra', 'Fundão', 'Aracruz', 'Linhares', 
            'Sooretama', 'Jaguaré', 'São Mateus', 'Conceição da Barra']

limit = "LIMIT 10" # argumento opcional para teste

### Extração dos dados

## RAIS

extrair_rais(
    anos = anos_relatorio,
    cidades = cidades_zeec,
    save_dir = proj_dir,
    ufs = uf,
    limit = limit
    )

## CAGED

extrair_caged(
    anos = anos_relatorio,
    cidades = cidades_zeec,
    save_dir = proj_dir,
    ufs = uf,
    mes = 12,
    limit = limit
    )

## ENEM
extrair_enem(
    anos = anos_relatorio,
    cidades = cidades_zeec,
    save_dir = proj_dir,
    ufs = uf,
    limit = limit
    )

## Censo

extrair_censo_pop(
    anos = anos_relatorio,
    cidades = cidades_zeec,
    save_dir = proj_dir,
    limit = limit
    )

extrair_censo_agua(
    anos = anos_relatorio,
    cidades = cidades_zeec,
    save_dir = proj_dir,
    limit = limit
    )

extrair_censo_esgoto(
    anos = anos_relatorio,
    cidades = cidades_zeec,
    save_dir = proj_dir,
    limit = limit
    )

extrair_censo_alfabetizados(
    anos = anos_relatorio,
    cidades = cidades_zeec,
    save_dir = proj_dir,
    limit = limit
    )


print("Processamento dos dados completo")