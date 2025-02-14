from pathlib import Path
from datetime import datetime
from src.web_scraper import get_latest_directory
from src.file_manager import list_zip_files, download_zip_files, extract_zip_files
from src.data_cleaner import convert_csv_to_parquet
from src.parquet_handler import merge_parquets

BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
entitys = [    "Empresas","Estabelecimentos","Cnae","Municipios","Simples"]

def main():
    #  Identificar diretório mais recente
    latest_dir = get_latest_directory(BASE_URL)
    
    raw_path = Path(f"./data/raw/{latest_dir}")  
    processed_path = Path(f"./data/processed/{latest_dir}")  
    final_output = Path(f"./data/final/final_{latest_dir}.parquet")  
    
    # Listar arquivoss
    zip_urls = list_zip_files(BASE_URL, latest_dir)
   
    for entity in entitys:
         #baixar os arquivos ZIP por entidade
        download_zip_files(zip_urls, raw_path,entity)
        # Extrair os arquivos CSV
        extract_zip_files(raw_path,entity)

        

    #  Carregar e limpar os dados, salvando em parqet temporarios
    for entity in entitys:
        convert_csv_to_parquet(raw_path,entity)


    # 6️⃣ Unificar Parquets
    merge_parquets(processed_path, final_output)

    print("Pipeline concluído!")

if __name__ == "__main__":
    main()
