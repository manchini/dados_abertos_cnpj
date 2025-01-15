
import pandas as pd
import os

def csv_to_parquet(directory):
    # List all CSV files in directory
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    
    colunas = ["CNPJ Base","Simples","Data Simmples","Data Simples Exc","MEI","Data MEI","Data Excl Mei",]

    for csv_file in csv_files:
        # Get file path
        csv_path = os.path.join(directory, csv_file)
        # Create parquet filename
        parquet_file = csv_file.replace('csv', '.parquet')
        parquet_path = os.path.join(directory, parquet_file)
        
        try:
            # Read CSV
            df = pd.read_csv(csv_path,encoding="latin-1",sep=";", names=colunas)

            df.drop(inplace=True,
                    columns=["Data Simmples","Data Simples Exc","Data MEI","Data Excl Mei"])
            # Write to parquet
            df.to_parquet(parquet_path, index=False)
            print(f"Converted {csv_file} to {parquet_file}")
        except Exception as e:
            print(f"Error converting {csv_file}: {str(e)}")


current_dir = os.getcwd()+"/simples/"
csv_to_parquet(current_dir)
