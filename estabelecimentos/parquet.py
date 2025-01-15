import pandas as pd
import os

def csv_to_parquet(directory):
    # List all CSV files in directory
    colunas = ["CNPJ Base","Filial","DV","Matriz","Nome Fantasia","Situacao","Data Cadastro",
               "motivo cadastro","Exterior","Pais","Inicio Atividade",
               "CNAE", "CNAE2", "Tipo Logra", "Logradouro","Numero","Complemento","Bairro",
               "CEP","UF","Municipio","ddd1","fone1","ddd2","fone2","ddd fax","Fax","Email","situacao receita","data situacao"]
    
    dtype_dict = {col: 'string' for col in colunas}
    
    
    csv_files = [f for f in os.listdir(directory) if f.endswith('ESTABELE')]
    
    for csv_file in csv_files:
        # Get file path
        csv_path = os.path.join(directory, csv_file)
        # Create parquet filename
        parquet_file = csv_file.replace('ESTABELE', 'ESTABELE.parquet')
        parquet_path = os.path.join(directory, parquet_file)
        
        try:
            # Read CSV
            print(f"Lendo CSV")
            df = pd.read_csv(csv_path,encoding="latin-1",sep=";", 
                             names= colunas ,low_memory=False)
            print("Apenas RS e ativas")
            df = df[(df["UF"] == "RS") & (df["Situacao"] == 2)]            

            print(f"DRop Colunas CSV")
            df.drop(inplace=True,
                columns=["Matriz","Data Cadastro","motivo cadastro","Exterior","Pais","Inicio Atividade",
             "Tipo Logra", "Logradouro","Numero","Complemento","Bairro","CEP","situacao receita","data situacao"])
            
            df.to_parquet(parquet_path, index=False)
            print(f"Converted {csv_file} to {parquet_file}")
        except Exception as e:
            print(f"Error converting {csv_file}: {str(e)}")


current_dir = os.getcwd()
csv_to_parquet(current_dir)
