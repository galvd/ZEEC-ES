from __future__ import annotations
import json
import sys
with open('.\\Arquivos\\config.json') as config_file:
    config = json.load(config_file)
    sys.path.append(config['caminho_rede'])
from os import getcwd, chdir

from Arquivos.ColetaDados.ExtratorRais import extrair_rais
from Arquivos.ColetaDados.ExtratorCaged import extrair_caged



anos = [2021, 2022]
cidades_zeec = ['Marataízes', 'Itapemirim', 'Cachoeiro de Itapemirim', 
            'Presidente Kennedy', 'Piúma', 'Anchieta', 'Guarapari', 
            'Viana', 'Vila Velha', 'Cariacica', 'Vitória', 
            'Serra', 'Fundão', 'Aracruz', 'Linhares', 
            'Sooretama', 'Jaguaré', 'São Mateus', 'Conceição da Barra']

limit = "LIMIT 10" # argumento opcional para teste


extrair_rais(anos = anos, cidades = cidades_zeec, save_dir = getcwd(), limit = limit)
extrair_caged(anos = anos, cidades = cidades_zeec,  save_dir = getcwd(), limit = limit)