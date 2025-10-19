import pandas as pd
import requests
import json

def get_gene_data_from_ensembl(gene_symbol="BRCA1"):
    """
    Получает информацию о гене из Ensembl API
    """
    base_url = "https://rest.ensembl.org"
    
    try:
        # Получаем ID гена по символу
        lookup_url = f"{base_url}/lookup/symbol/human/{gene_symbol}"
        lookup_response = requests.get(lookup_url, headers={"Content-Type": "application/json"})
        lookup_response.raise_for_status()
        gene_info = lookup_response.json()
        
        gene_id = gene_info['id']
        
        # Получаем дополнительную информацию о гене
        gene_url = f"{base_url}/lookup/id/{gene_id}"
        gene_response = requests.get(gene_url, headers={"Content-Type": "application/json"})
        gene_response.raise_for_status()
        gene_data = gene_response.json()
        
        return gene_data
        
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к Ensembl API: {e}")
        return None

def get_multiple_genes_data():
    """
    Получает информацию о нескольких генах
    """
    genes = ["BRCA1", "TP53", "EGFR", "CFTR", "HBB"]
    genes_data = []
    
    for gene in genes:
        print(f"Загрузка данных для гена {gene}...")
        gene_data = get_gene_data_from_ensembl(gene)
        
        if gene_data:
            # Извлекаем нужные поля
            formatted_data = {
                'gene_symbol': gene,
                'ensembl_id': gene_data.get('id', 'N/A'),
                'description': gene_data.get('description', 'N/A'),
                'chromosome': gene_data.get('seq_region_name', 'N/A'),
                'start_position': gene_data.get('start', 'N/A'),
                'end_position': gene_data.get('end', 'N/A'),
                'strand': gene_data.get('strand', 'N/A'),
                'biotype': gene_data.get('biotype', 'N/A')
            }
            genes_data.append(formatted_data)
    
    return genes_data

def main():
    print("=" * 60)
    print("Ensembl Genomics API Data Loader")
    print("=" * 60)
    
    # Получаем данные о генах
    genes_data = get_multiple_genes_data()
    
    if genes_data:
        # Создаем DataFrame
        df = pd.DataFrame(genes_data)
        
        print("\nДанные успешно загружены")
        print(f"Размер таблицы: {df.shape}")
        
        print("\nТаблица генов:")
        print("=" * 80)
        print(df.to_string(index=False))
        
        print("\nИнформация о данных:")
        print("=" * 40)
        print(df.info())
        
        print("\nРаспределение генов по хромосомам:")
        print("=" * 40)
        print(df['chromosome'].value_counts())
        
        return df
    else:
        print("Не удалось загрузить данные")
        return None

if __name__ == "__main__":
    df = main()