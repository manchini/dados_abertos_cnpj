import pandas as pd
from pathlib import Path

coluns = {
    "Empresas": ["CNPJ Base","Razao Social","Natureza","Resp","capital","porte","ente federativo"],
    "Estabelecimentos": ["CNPJ Base","Filial","DV","Matriz","Nome Fantasia","Situacao","Data Cadastro",
               "motivo cadastro","Exterior","Pais","Inicio Atividade",
               "CNAE", "CNAE2", "Tipo Logra", "Logradouro","Numero","Complemento","Bairro",
               "CEP","UF","Municipio","ddd1","fone1","ddd2","fone2","ddd fax","Fax","Email","situacao receita","data situacao"],
    "Simples": ["CNPJ Base","Simples","Data Simmples","Data Simples Exc","MEI","Data MEI","Data Excl Mei",],
    "Cnae": ["CNAE","Nome CNAE"],
    "Municipios": ["Municipio","Nome Municipio"]
}
columns_to_remove = {
    "Empresas": ["Natureza","Resp","capital","porte","ente federativo"],
    "Estabelecimentos": ["Matriz","Data Cadastro","motivo cadastro","Exterior","Pais",
             "Tipo Logra", "Logradouro","Numero","Complemento","Bairro","CEP","situacao receita","data situacao"],
    "Simples":["Data Simmples","Data Simples Exc","Data MEI","Data Excl Mei"],
    "Cnae": [],
    "Municipios": []
}

def convert_csv_to_parquet(folder_path,entity):
    """ Converte arquivos CSV em Parquet """
    folder_entity = Path(f'{folder_path}/{entity}')
    print(folder_entity)
    csv_files = list(folder_entity.glob("*"))
    csv_files = [csv for csv in csv_files if not str(csv).endswith(".zip") ]
    csv_files = [csv for csv in csv_files if not str(csv).endswith(".parquet") ]

    for csv in csv_files:
       df = load_csv_files(csv,entity)
       clean_dataframe(df,entity)
       parquet_file = csv.with_suffix('.parquet').replace("raw","processed")
       save_parquet(df,parquet_file)
       

def clean_dataframe(dataframe, entity):
    """ Limpa e mantém apenas as colunas desejadas """
    dataframe.drop(inplace=True,columns=columns_to_remove[entity])

def load_csv_files(csv,entity):
    """ Carrega os arquivos CSV e retorna DataFrame """ 
    dtype_dict = {col: 'string' for col in coluns[entity]}
    dataframe = pd.read_csv(csv,encoding="latin-1",sep=";",
                            low_memory=False,
                            names=coluns[entity],
                            dtype=dtype_dict
                            )
    return dataframe

def save_parquet(dataframe, output_path):
    dataframe.to_parquet(output_path, index=False)
    