import pyarrow.parquet as pq
import pandas as pd
import os

def explore_parquet(file_path):
    """
    Explora metadatos y muestra las primeras filas de un archivo Parquet de forma segura.
    """
    print(f"=== Exploración de Datos: {file_path} ===")
    
    # 1. Metadatos (Sin cargar datos)
    parquet_file = pq.ParquetFile(file_path)
    print("\n[Metadatos]")
    print(f" - Total de filas: {parquet_file.metadata.num_rows:,}")
    print(f" - Columnas: {len(parquet_file.schema.names)}")
    print(f" - Grupos de filas (Row Groups): {parquet_file.num_row_groups}")
    
    # 2. Muestra de Datos (Cargando solo las primeras 5 filas)
    print("\n[Muestra de las primeras 5 filas]")
    # Usamos slice(0, 5) para leer estrictamente lo necesario
    table_sample = parquet_file.read_row_group(0).slice(0, 5)
    df_sample = table_sample.to_pandas()
    
    # Mostramos transpuesta para leer todas las columnas cómodamente
    pd.set_option('display.max_columns', None)
    print(df_sample.transpose())

def create_csv_sample(parquet_path, csv_path, n_rows=50000):
    """
    Crea un archivo CSV pequeño a partir del Parquet para comparativas de rendimiento.
    """
    print(f"\n=== Generando muestra CSV: {csv_path} ({n_rows} filas) ===")
    
    if os.path.exists(csv_path):
        print(f" ! El archivo {csv_path} ya existe. Saltando generación.")
        return

    # Leemos solo el número de filas solicitado
    parquet_file = pq.ParquetFile(parquet_path)
    # Leemos el primer row group y cortamos
    table = parquet_file.read_row_group(0).slice(0, n_rows)
    df = table.to_pandas()
    
    df.to_csv(csv_path, index=False)
    print(f" ✓ Muestra CSV creada con éxito. Tamaño: {os.path.getsize(csv_path) / (1024*1024):.2f} MB")

if __name__ == "__main__":
    PARQUET_PATH = "data/raw/Combined_Flights_2022.parquet"
    CSV_SAMPLE_PATH = "data/raw/flights_sample.csv"
    
    if os.path.exists(PARQUET_PATH):
        explore_parquet(PARQUET_PATH)
        create_csv_sample(PARQUET_PATH, CSV_SAMPLE_PATH)
    else:
        print(f"Error: No se encuentra {PARQUET_PATH}")
