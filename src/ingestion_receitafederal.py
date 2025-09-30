import os
import requests
import zipfile
import io
from tqdm.auto import tqdm # Importa a biblioteca de progresso

# Diretório para os arquivos ZIP
DOWNLOAD_DIR = "data/bronze/zip"
#Diretório para os arquivos extraídos
EXTRACTION_DIR = "data/bronze/extraction"

URLS = {
    #URLs obtidas a partir do site da Receita Federal
    "Empresas": "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/2025-06/Empresas8.zip",
    "Socios": "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/2025-06/Socios8.zip"
}

def setup_directories():
    """Cria os diretórios necessários (data/bronze/zip e data/bronze/extraction)."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(EXTRACTION_DIR, exist_ok=True)
    print(f"Diretórios configurados: {DOWNLOAD_DIR} e {EXTRACTION_DIR}")

def download_and_extract_zips():
    """Baixando os arquivos ZIP e extraindo seu conteúdo, com acompanhamento do progresso."""
    setup_directories()
    
    arquivos_extraidos = []

    for nome, url in URLS.items():
        zip_filename = os.path.join(DOWNLOAD_DIR, url.split('/')[-1])
        
        print(f"\n--- Iniciando o download de {nome} ---")
        try:
            # 1. Download
            response = requests.get(url, stream=True)
            response.raise_for_status() # Lança erro para status de erro HTTP

            #como o download é demorado, foi incluído uma barra de carregamento para validar que o processo está rodando e não travado
            # Calcula o tamanho total para a barra de progresso
            total_size = int(response.headers.get('content-length', 0))
            block_size = 8192
            
            # Salva o arquivo ZIP com barra de progresso
            with open(zip_filename, 'wb') as f:
                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Baixando {nome}", colour='green') as t:
                    for chunk in response.iter_content(chunk_size=block_size):
                        t.update(len(chunk))
                        f.write(chunk)

            print(f"Arquivo ZIP baixado: {zip_filename}")

            # 2. Extração
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                print(f"Extraindo arquivos CSV de {zip_filename}...")
                for member in zip_ref.namelist():
                    # Extrai apenas os arquivos que nos interessam neste desafio (EMPRECSV e SOCIOCSV)
                    if 'EMPRECSV' in member.upper() or 'SOCIOCSV' in member.upper():
                        zip_ref.extract(member, EXTRACTION_DIR)
                        # O nome do arquivo extraído fica com o caminho do ZIP, ajustadopara a pasta de extração
                        extracted_path = os.path.join(EXTRACTION_DIR, member)
                        arquivos_extraidos.append(extracted_path)
                        print(f"Extraído: {member}")

        except requests.exceptions.RequestException as e:
            print(f"ERRO: Não foi possível baixar {nome} ({url}): {e}")
        except zipfile.BadZipFile:
            print(f"ERRO: O arquivo ZIP {zip_filename} está corrompido ou o download foi interrompido.")
        except Exception as e:
            print(f"ERRO inesperado durante o download ou extração: {e}")

    return arquivos_extraidos

if __name__ == "__main__":
    download_and_extract_zips()
