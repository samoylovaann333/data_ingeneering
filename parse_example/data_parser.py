import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

def parse_ncbi_genes():
    """
    Парсит информацию о генах с NCBI Gene database
    """
    print("Парсинг данных о генах с NCBI...")
    
    # Список генов для парсинга (те же что и в API задании для консистентности)
    genes = ["BRCA1", "TP53", "EGFR", "CFTR", "HBB"]
    
    genes_data = []
    
    for gene in genes:
        print(f"Парсим данные для гена {gene}...")
        
        try:
            # URL страницы гена на NCBI
            url = f"https://www.ncbi.nlm.nih.gov/gene/?term={gene}[gene]+AND+human[orgn]"
            
            # Заголовки чтобы избежать блокировки
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            
            # Запрос к странице
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            # Парсим HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем основную информацию о гене
            gene_info = extract_gene_info(soup, gene)
            genes_data.append(gene_info)
            
            # Пауза между запросами
            time.sleep(2)
            
        except Exception as e:
            print(f"Ошибка при парсинге гена {gene}: {e}")
            # Добавляем базовую информацию даже при ошибке
            genes_data.append({
                'gene_symbol': gene,
                'gene_id': 'N/A',
                'chromosome': 'N/A',
                'location': 'N/A',
                'description': f'Error: {str(e)}',
                'source': 'NCBI'
            })
    
    return genes_data

def extract_gene_info(soup, gene_symbol):
    """
    Извлекает информацию о гене из HTML
    """
    try:
        # Ищем Gene ID (обычно в мета-тегах или заголовке)
        gene_id = "N/A"
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            if 'geneid' in str(meta).lower():
                content = meta.get('content', '')
                if content.isdigit():
                    gene_id = content
                    break
        
        # Ищем описание гена
        description = "N/A"
        desc_elem = soup.find('dt', string='Summary')
        if desc_elem:
            desc_elem = desc_elem.find_next_sibling('dd')
            if desc_elem:
                description = desc_elem.get_text(strip=True)
        
        # Ищем локализацию
        chromosome = "N/A"
        location = "N/A"
        loc_elem = soup.find('dt', string='Location')
        if loc_elem:
            loc_elem = loc_elem.find_next_sibling('dd')
            if loc_elem:
                location_text = loc_elem.get_text(strip=True)
                if 'chromosome' in location_text.lower():
                    chromosome = location_text.split('chromosome')[-1].strip()
                    location = location_text
        
        return {
            'gene_symbol': gene_symbol,
            'gene_id': gene_id,
            'chromosome': chromosome,
            'location': location,
            'description': description[:200] + '...' if len(description) > 200 else description,
            'source': 'NCBI'
        }
        
    except Exception as e:
        print(f"Ошибка при извлечении информации для {gene_symbol}: {e}")
        return {
            'gene_symbol': gene_symbol,
            'gene_id': 'N/A',
            'chromosome': 'N/A', 
            'location': 'N/A',
            'description': f'Extraction error: {str(e)}',
            'source': 'NCBI'
        }

def main():
    print("=" * 60)
    print("NCBI Gene Data Parser")
    print("=" * 60)
    
    # Парсим данные
    genes_data = parse_ncbi_genes()
    
    if genes_data:
        # Создаем DataFrame
        df = pd.DataFrame(genes_data)
        
        print("\nДанные успешно распарсены")
        print(f"Размер таблицы: {df.shape}")
        
        print("\nТаблица генов:")
        print("=" * 80)
        print(df.to_string(index=False))
        
        print("\nИнформация о данных:")
        print("=" * 40)
        print(f"Колонки: {list(df.columns)}")
        print(f"Всего записей: {len(df)}")
        
        print("\nСтатистика по хромосомам:")
        print("=" * 40)
        print(df['chromosome'].value_counts())
        
        return df
    else:
        print("Не удалось распарсить данные")
        return None

if __name__ == "__main__":
    df = main()
