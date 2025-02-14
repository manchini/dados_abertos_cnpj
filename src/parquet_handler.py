from pathlib import Path
import pandas as pd
import os
import glob



def merge_parquets(processed_path, final_output):
  
    # Unifica os arquivos Parquet em um único data frame 
    parquet_estabelecimentos_files = glob.glob(Path(f'{processed_path}/Estabelecimentos/'))
    df_estabelecimentos = pd.concat([pd.read_parquet(f) for f in parquet_estabelecimentos_files], ignore_index=True)

    parquet_empresa_files = glob.glob(Path(f'{processed_path}/Empresas/'))
    df_empresas = pd.concat([pd.read_parquet(f) for f in parquet_empresa_files], ignore_index=True)

    parquet_municipios_files = glob.glob(Path(f'{processed_path}/Municipios/'))
    df_municipios = pd.concat([pd.read_parquet(f) for f in parquet_municipios_files], ignore_index=True)

    parquet_cnae_files = glob.glob(Path(f'{processed_path}/Municipios/'))
    df_cnae = pd.concat([pd.read_parquet(f) for f in parquet_cnae_files], ignore_index=True)

    parquet_simples_files = glob.glob(Path(f'{processed_path}/Simples/'))
    df_simples = pd.concat([pd.read_parquet(f) for f in parquet_simples_files], ignore_index=True)

    # Converte os tipos de dados
    df_empresas["CNPJ Base"] = df_empresas["CNPJ Base"].astype(str)
    df_estabelecimentos["CNPJ Base"] = df_estabelecimentos["CNPJ Base"].astype(str)
    df_simples["CNPJ Base"] = df_simples["CNPJ Base"].astype(str)

    # Mescla os dataframes
    df_estabelecimentos = pd.merge(df_estabelecimentos, df_empresas, on='CNPJ Base', how='left')
    df_estabelecimentos = pd.merge(df_estabelecimentos, df_simples, on='CNPJ Base', how='left') 
    df_estabelecimentos = pd.merge(df_estabelecimentos, df_municipios, on='Municipio', how='left')
    df_estabelecimentos = pd.merge(df_estabelecimentos, df_cnae, on='CNAE', how='left')

    # Converte os tipos de dados
    df_estabelecimentos["ddd1"] = df_estabelecimentos["ddd1"].astype(str)
    df_estabelecimentos["ddd2"] = df_estabelecimentos["ddd2"].astype(str)
    df_estabelecimentos["ddd fax"] = df_estabelecimentos["ddd fax"].astype(str)

    # Salva o resultado em um novo arquivo Parquet
    df_estabelecimentos.to_parquet(final_output)