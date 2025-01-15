
import pandas as pd
import os

def csv_to_parquet(directory):
    # List all CSV files in directory
    csv_files = [f for f in os.listdir(directory) if f.endswith('CSV')]

    colunas = ["CNPJ Base","Razao Social","Natureza","Resp","capital","porte","ente federativo"]
    
    for csv_file in csv_files:
        # Get file path
        csv_path = os.path.join(directory, csv_file)
        # Create parquet filename
        parquet_file = csv_file.replace('CSV', '.parquet')
        parquet_path = os.path.join(directory, parquet_file)
        
        try:
            # Read CSV
            print(f"Lendo")
            df = pd.read_csv(csv_path,encoding="latin-1",sep=";",names=colunas)
            
            print(f"Removendo colunas")
            df.drop(inplace=True,
                columns=["Natureza","Resp","capital","porte","ente federativo"])
            # Write to parquet
            df.to_parquet(parquet_path, index=False)
            print(f"Converted {csv_file} to {parquet_file}")
        except Exception as e:
            print(f"Error converting {csv_file}: {str(e)}")


current_dir = os.getcwd()
csv_to_parquet(current_dir)
