"""
Products Routes
Маршруты для работы с товарами
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional

from services.product_service import ProductService

router = APIRouter()
product_service = ProductService()


@router.get("/")
async def get_products(
    skip: int = Query(0, description="Количество пропускаемых записей"),
    limit: int = Query(100, description="Максимальное количество записей"),
    category: Optional[str] = Query(None, description="Фильтр по категории"),
    brand: Optional[str] = Query(None, description="Фильтр по бренду"),
    min_price: Optional[float] = Query(None, description="Минимальная цена"),
    max_price: Optional[float] = Query(None, description="Максимальная цена")
):
    """Получение списка товаров"""
    try:
        products = await product_service.get_products(
            skip=skip,
            limit=limit,
            category=category,
            brand=brand,
            min_price=min_price,
            max_price=max_price
        )
        return products
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{product_id}")
async def get_product(product_id: int):
    """Получение информации о товаре"""
    try:
        product = await product_service.get_product_by_id(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Товар не найден")
        return product
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{product_id}/profile")
async def get_product_profile(product_id: int):
    """Получение профиля товара"""
    try:
        profile = await product_service.get_product_profile(product_id)
        if not profile:
            raise HTTPException(status_code=404, detail="Товар не найден")
        return profile
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{product_id}/price-history")
async def get_product_price_history(
    product_id: int,
    days: int = Query(30, description="Количество дней истории")
):
    """Получение истории цен товара"""
    try:
        history = await product_service.get_price_history(
            product_id=product_id,
            days=days
        )
        return history
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories/")
async def get_categories():
    """Получение списка категорий"""
    try:
        categories = await product_service.get_categories()
        return categories
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/brands/")
async def get_brands():
    """Получение списка брендов"""
    try:
        brands = await product_service.get_brands()
        return brands
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/popular/")
async def get_popular_products(
    period: str = Query("30d", description="Период популярности"),
    limit: int = Query(10, description="Количество товаров")
):
    """Получение популярных товаров"""
    try:
        popular = await product_service.get_popular_products(
            period=period,
            limit=limit
        )
        return popular
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/")
async def search_products(
    query: str = Query(..., description="Поисковый запрос"),
    limit: int = Query(20, description="Максимальное количество результатов")
):
    """Поиск товаров"""
    try:
        results = await product_service.search_products(
            query=query,
            limit=limit
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

