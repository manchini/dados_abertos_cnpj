import os
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import zipfile

def list_zip_files(base_url, directory):
    """ Obtém a lista de arquivos ZIP dentro do diretório especificado """
    url = f"{base_url}/{directory}/"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar {url}")

    soup = BeautifulSoup(response.text, "html.parser")
    zip_files = [link["href"] for link in soup.find_all("a", href=True) if link["href"].endswith(".zip")]
    
    return [f"{url}{file}" for file in zip_files]

def download_zip_files(zip_urls, save_folder, entity):
    """ Baixa os arquivos ZIP e salva na pasta especificada """

    save_folder_entity = Path(f'{save_folder}/{entity}')
    save_folder_entity.mkdir(parents=True, exist_ok=True)

    zip_entity = pd.DataFrame(zip_urls, columns=["url"])
    zip_entity = zip_entity[(zip_entity["url"].str.contains(entity))]
    
    for url in zip_entity["url"]:
        
        filename = url.split("/")[-1]
        file_path = save_folder_entity / filename

        print(f"Baixando arquivo {url} para {file_path}")

        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Baixado: {filename}")
        else:
            print(f"Erro ao baixar {filename}: {response.status_code}")

def extract_zip_files(folder_path,entity):
    """ Extrai todos os arquivos ZIP dentro de uma pasta """
    folder_entity = Path(f'{folder_path}/{entity}')
    zip_files = list(folder_entity.glob("*.zip"))

    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(folder_entity)
        print(f"Extraído: {zip_file.name}")

