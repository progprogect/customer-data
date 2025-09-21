#!/usr/bin/env python3
"""
Simple bot runner to avoid event loop issues
"""

import asyncio
import logging
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import main

if __name__ == "__main__":
    try:
        # Run with asyncio.run() - this creates a new event loop
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\n🛑 Bot остановлен пользователем")
    except Exception as e:
        print(f"❌ Ошибка запуска бота: {e}")