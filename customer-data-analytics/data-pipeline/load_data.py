#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data loader for Customer Data Analytics
Загружает сгенерированные данные в PostgreSQL базу данных

Author: Customer Data Analytics Team
"""

import os
import subprocess
import sys
from pathlib import Path

# Конфигурация
DATABASE_NAME = "customer_data"
SEED_DIR = "seed"
HOST = "localhost"
PORT = "5432"
USER = "mikitavalkunovich"

def run_sql_file(file_path: str, description: str):
    """Выполняет SQL файл и выводит результат"""
    print(f"🔄 {description}...")
    
    try:
        result = subprocess.run([
            "psql", 
            "-h", HOST,
            "-p", PORT,
            "-U", USER,
            "-d", DATABASE_NAME,
            "-f", file_path
        ], capture_output=True, text=True, check=True)
        
        print(f"✅ {description} - успешно")
        if result.stdout:
            print(f"📊 Результат: {result.stdout.strip()}")
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка при {description}")
        print(f"Код ошибки: {e.returncode}")
        print(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("❌ psql не найден. Убедитесь, что PostgreSQL установлен и добавлен в PATH")
        return False
    
    return True

def check_database_exists():
    """Проверяет существование базы данных"""
    try:
        result = subprocess.run([
            "psql", 
            "-h", HOST,
            "-p", PORT,
            "-U", USER,
            "-d", DATABASE_NAME,
            "-c", "SELECT 1;"
        ], capture_output=True, text=True, check=True)
        
        return True
    except:
        return False

def main():
    print("🚀 Загрузка тестовых данных в Customer Data Analytics")
    print(f"📊 База данных: {DATABASE_NAME}")
    print(f"📁 Директория данных: {SEED_DIR}")
    
    # Проверяем существование директории с данными
    if not os.path.exists(SEED_DIR):
        print(f"❌ Директория {SEED_DIR} не найдена")
        print("Сначала запустите generate_data.py для генерации данных")
        sys.exit(1)
    
    # Проверяем существование базы данных
    if not check_database_exists():
        print(f"❌ База данных {DATABASE_NAME} не существует")
        print("Сначала создайте базу данных:")
        print(f"  createdb {DATABASE_NAME}")
        sys.exit(1)
    
    # Список файлов для загрузки в правильном порядке
    files_to_load = [
        ("01_categories.sql", "Загрузка категорий товаров"),
        ("00_copy.sql", "Загрузка основных данных (товары, пользователи, заказы, события)")
    ]
    
    # Загружаем файлы
    success = True
    for filename, description in files_to_load:
        file_path = os.path.join(SEED_DIR, filename)
        
        if not os.path.exists(file_path):
            print(f"❌ Файл {file_path} не найден")
            success = False
            continue
        
        if not run_sql_file(file_path, description):
            success = False
            break
    
    if success:
        print("\n✅ Все данные успешно загружены!")
        
        # Запускаем валидацию
        validate_file = os.path.join(SEED_DIR, "99_validate.sql")
        if os.path.exists(validate_file):
            print("\n🔍 Запуск валидации данных...")
            run_sql_file(validate_file, "Валидация данных")
        
        print("\n🎉 Готово! Тестовые данные загружены в базу данных")
        print("\n📋 Следующие шаги:")
        print("  1. Запустите API сервер: cd api && python main.py")
        print("  2. Запустите фронтенд: cd frontend && npm start")
        print("  3. Запустите Telegram бот: cd telegram-bot && python main.py")
        
    else:
        print("\n❌ Ошибка при загрузке данных")
        sys.exit(1)

if __name__ == "__main__":
    main()
