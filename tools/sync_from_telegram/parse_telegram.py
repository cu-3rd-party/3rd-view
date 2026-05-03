import json
import re

def extract_ktalk_links(json_file_path, output_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Регулярка для поиска ID записи Толка
    # Ищет форматы: centraluniversity.ktalk.ru/recordings/ID
    pattern = re.compile(r'centraluniversity\.ktalk\.ru/recordings/([a-zA-Z0-9]+)')
    
    unique_links = set()

    for message in data.get('messages',[]):
        text_data = message.get('text', '')
        
        # В Telegram JSON текст может быть строкой или массивом сущностей
        if isinstance(text_data, str):
            matches = pattern.findall(text_data)
            unique_links.update(matches)
        
        elif isinstance(text_data, list):
            for entity in text_data:
                if isinstance(entity, str):
                    matches = pattern.findall(entity)
                    unique_links.update(matches)
                elif isinstance(entity, dict):
                    # Проверяем сам текст сущности
                    if 'text' in entity:
                        matches = pattern.findall(entity['text'])
                        unique_links.update(matches)
                    # Если это скрытая гиперссылка, ссылка будет в href
                    if 'href' in entity:
                        matches = pattern.findall(entity['href'])
                        unique_links.update(matches)

    # Сохраняем в файл
    with open(output_file_path, 'w', encoding='utf-8') as f:
        for link_id in unique_links:
            f.write(f"https://centraluniversity.ktalk.ru/recordings/{link_id}\n")

    print(f"✅ Готово! Найдено уникальных записей: {len(unique_links)}. Сохранено в {output_file_path}")

if __name__ == "__main__":
    # Запуск парсера
    extract_ktalk_links('result.json', 'parsed_records.txt')