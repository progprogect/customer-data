#!/usr/bin/env python3
"""
Скрипт для исправления относительных импортов в API
"""

import os
import re

def fix_imports_in_file(file_path):
    """Исправляет относительные импорты в файле"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Заменяем относительные импорты на абсолютные
    content = re.sub(r'from \.\.', 'from ', content)
    content = re.sub(r'from \.', 'from ', content)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Fixed imports in {file_path}")

def main():
    """Основная функция"""
    api_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Список файлов для исправления
    files_to_fix = [
        'main.py',
        'routes/analytics.py',
        'routes/recommendations.py',
        'routes/users.py',
        'routes/products.py',
        'routes/segments.py',
        'services/analytics_service.py',
        'services/database.py',
        'services/product_service.py',
        'services/recommendation_service.py',
        'services/user_service.py',
        'middleware/logging.py',
        'models/analytics.py'
    ]
    
    for file_path in files_to_fix:
        full_path = os.path.join(api_dir, file_path)
        if os.path.exists(full_path):
            fix_imports_in_file(full_path)
        else:
            print(f"File not found: {full_path}")

if __name__ == "__main__":
    main()

