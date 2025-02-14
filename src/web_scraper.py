import requests
from bs4 import BeautifulSoup
import re

def get_latest_directory(base_url):
    """ Obtém o diretório mais recente da web no formato YYYY-MM """
    response = requests.get(base_url)
    if response.status_code != 200:
        raise Exception("Não foi possível acessar a URL")

    soup = BeautifulSoup(response.text, "html.parser")
    directories = []

    # Procura diretórios no formato YYYY-MM/
    for link in soup.find_all("a", href=True):
        match = re.match(r"(\d{4}-\d{2})/", link["href"])
        if match:
            directories.append(match.group(1))

    if not directories:
        raise Exception("Nenhum diretório encontrado")

    latest_dir = sorted(directories, reverse=True)[0]
    return latest_dir
