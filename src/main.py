# Importa as funções de cada etapa (utilizando importação absoluta)
from src.ingestion_receitafederal import download_and_extract_zips
from src.load_raw import load_raw
from src.show_deltaraw import show_deltaraw
from src.transform_silver import transform_silver
from src.show_deltasilver import show_deltasilver
from src.agg_gold import builder_gold
from src.show_deltagold import show_deltagold
from src.database import load_postgres

def run_challenge():
    """Função principal que orquestra o fluxo completo do desafio."""
    print("Iniciando o desafio de Engenharia de Dados - Modelo Medalhão...")
    
    # --- Etapa 1: Extração dos arquivos da receita Federal (Camada BRONZE) ---
    print("\n--- Etapa 1: Extração dos arquivos da Receita Federal (Camada BRONZE) ---")
    download_and_extract_zips()

    # --- Etapa 2: Leitura e Carregamento para Delta (Camada RAW) ---
    print("\n--- Etapa 2: Carregamento para Camada RAW (Delta Lake) ---")
    load_raw()

    #Amostragem das tabelas
    print("\n--- Amostragem das tabelas - RAW ---")
    show_deltaraw()

    print("\n--- Etapa BRONZE concluída com sucesso! ---")

    # --- Etapa 3: Transformação e Limpeza (Camada SILVER) ---
    print("\n--- Etapa 3: Transformação e Limpeza (Camada SILVER) ---")
    transform_silver()

    #Amostragem das tabelas
    print("\n--- Amostragem das tabelas - SILVER ---")
    show_deltasilver()

    # --- Etapa 4: Agregação e Indicadores (Camada GOLD) ---
    print("\n--- Etapa 4: Agregação (Camada GOLD) ---")
    builder_gold()

    #Amostragem das tabelas
    print("\n--- Amostragem das tabelas - GOLD ---")
    show_deltagold()

    # --- Etapa 5: Carga da tabela final no banco de dados ---
    print("\n--- Carga da tabela final no PostgreSQL ---")
    load_postgres()
    
if __name__ == "__main__":
    run_challenge()