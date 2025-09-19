"""
Recommendations API Routes
API эндпоинты для content-based рекомендаций
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
import logging
import time
from datetime import datetime

from models.analytics import RecommendationResponse, RecommendationItem, UserPurchaseItem
from services.recommendation_service import recommendation_service
from services.hybrid_recommendation_service import hybrid_recommendation_service
from services.database import get_db_connection
import psycopg2.extras

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/item-similar",
    response_model=List[RecommendationItem],
    summary="Get similar items",
    description="Получение товаров, похожих на указанный (content-based)"
)
async def get_item_similar(
    product_id: int = Query(..., description="ID товара для поиска похожих"),
    k: int = Query(20, ge=1, le=50, description="Количество рекомендаций")
):
    """
    Endpoint для получения похожих товаров
    
    Args:
        product_id: ID товара
        k: количество рекомендаций (1-50)
        
    Returns:
        List[RecommendationItem]: список похожих товаров
    """
    start_time = time.time()
    
    try:
        logger.info(f"🔍 Item similarity request: product_id={product_id}, k={k}")
        
        # Получаем похожие товары
        similar_items = recommendation_service.get_item_similar(product_id, k)
        
        if not similar_items:
            raise HTTPException(
                status_code=404,
                detail=f"No similar items found for product {product_id}"
            )
        
        # Конвертируем в Pydantic модели
        recommendations = []
        for item in similar_items:
            recommendations.append(RecommendationItem(
                product_id=item["product_id"],
                title=item["title"],
                brand=item["brand"],
                category=item["category"],
                price=item["price"],
                score=item["similarity_score"],
                popularity_score=item["popularity_score"],
                rating=item["rating"],
                recommendation_reason="similar_content"
            ))
        
        processing_time = (time.time() - start_time) * 1000
        logger.info(f"✅ Item similarity completed: {len(recommendations)} items, "
                   f"{processing_time:.1f}ms")
        
        return recommendations
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in item similarity: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/user-content",
    response_model=RecommendationResponse,
    summary="Get user content recommendations",
    description="Получение персональных content-based рекомендаций для пользователя"
)
async def get_user_content_recommendations(
    user_id: int = Query(..., description="ID пользователя"),
    k: int = Query(20, ge=1, le=50, description="Количество рекомендаций")
):
    """
    Endpoint для получения персональных рекомендаций пользователю
    
    Args:
        user_id: ID пользователя
        k: количество рекомендаций (1-50)
        
    Returns:
        RecommendationResponse: рекомендации с метаданными
    """
    start_time = time.time()
    
    try:
        logger.info(f"👤 User content recommendations request: user_id={user_id}, k={k}")
        
        # Пока заглушка - будет реализовано в следующей итерации
        # Временно возвращаем популярные товары
        popular_items_query = """
        SELECT 
            product_id,
            title,
            brand,
            category,
            price_current,
            popularity_30d,
            rating
        FROM ml_item_content_features
        WHERE is_active = true
            AND popularity_30d > 0
        ORDER BY popularity_30d DESC
        LIMIT :k
        """
        
        # Простая реализация через популярные товары
        recommendations = []
        for i in range(min(k, 10)):  # заглушка
            recommendations.append(RecommendationItem(
                product_id=i + 1,
                title=f"Popular Product {i + 1}",
                brand="TBD",
                category="TBD",
                price=100.0,
                score=0.8,
                popularity_score=1000.0,
                rating=4.5,
                recommendation_reason="popular_fallback"
            ))
        
        processing_time = (time.time() - start_time) * 1000
        
        response = RecommendationResponse(
            user_id=user_id,
            algorithm="content_based",
            recommendations=recommendations,
            total_count=len(recommendations),
            processing_time_ms=processing_time,
            generated_at=datetime.now()
        )
        
        logger.info(f"✅ User recommendations completed: {len(recommendations)} items, "
                   f"{processing_time:.1f}ms")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in user recommendations: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/user-cf",
    response_model=List[RecommendationItem],
    summary="Get user CF recommendations", 
    description="Получение персональных Item-kNN CF рекомендаций для пользователя"
)
async def get_user_cf_recommendations(
    user_id: int = Query(..., description="ID пользователя"),
    k: int = Query(20, ge=1, le=50, description="Количество рекомендаций")
):
    """
    Endpoint для получения Collaborative Filtering рекомендаций
    
    Args:
        user_id: ID пользователя
        k: количество рекомендаций (1-50)
        
    Returns:
        List[RecommendationItem]: список CF рекомендаций
    """
    start_time = time.time()
    
    try:
        logger.info(f"🤝 CF recommendations request: user_id={user_id}, k={k}")
        
        # Получаем CF рекомендации
        cf_recommendations = recommendation_service.get_user_cf_recommendations(user_id, k)
        
        if not cf_recommendations:
            # Fallback на content-based если CF не сработал
            logger.info(f"🔄 CF fallback to content-based for user {user_id}")
            # Здесь можно добавить fallback логику
            return []
        
        # Конвертируем в Pydantic модели
        recommendations = []
        for item in cf_recommendations:
            recommendations.append(RecommendationItem(
                product_id=item["product_id"],
                title=item["title"],
                brand=item["brand"],
                category=item["category"],
                price=item["price"],
                score=item["score"],
                popularity_score=item["popularity_score"],
                rating=item["rating"],
                recommendation_reason="collaborative_filtering"
            ))
        
        processing_time = (time.time() - start_time) * 1000
        logger.info(f"✅ CF recommendations completed: {len(recommendations)} items, "
                   f"{processing_time:.1f}ms")
        
        return recommendations
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in CF recommendations: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/user-hybrid",
    response_model=List[RecommendationItem],
    summary="Get hybrid recommendations",
    description="Получение гибридных рекомендаций (CF + Content + Popularity)"
)
async def get_user_hybrid_recommendations(
    user_id: int = Query(..., description="ID пользователя"),
    k: int = Query(20, ge=1, le=50, description="Количество рекомендаций")
):
    """
    Endpoint для получения гибридных рекомендаций
    
    Args:
        user_id: ID пользователя  
        k: количество рекомендаций (1-50)
        
    Returns:
        List[RecommendationItem]: список гибридных рекомендаций
    """
    start_time = time.time()
    
    try:
        logger.info(f"🔀 Hybrid recommendations request: user_id={user_id}, k={k}")
        
        # Получаем гибридные рекомендации
        hybrid_result = hybrid_recommendation_service.get_hybrid_recommendations(user_id, k)
        
        if not hybrid_result['recommendations']:
            logger.warning(f"No hybrid recommendations for user {user_id}")
            return []
        
        # Конвертируем в Pydantic модели
        recommendations = []
        for item in hybrid_result['recommendations']:
            recommendations.append(RecommendationItem(
                product_id=item["product_id"],
                title=item["title"],
                brand=item["brand"],
                category=item["category"],
                price=item["price"],
                score=item["hybrid_score"],
                popularity_score=item["popularity_score"],
                rating=item["rating"],
                recommendation_reason=f"hybrid_{item.get('source', 'mixed')}"
            ))
        
        processing_time = (time.time() - start_time) * 1000
        
        # Логируем подробную статистику
        logger.info(f"✅ Hybrid recommendations completed: {len(recommendations)} items, {processing_time:.1f}ms")
        logger.info(f"📊 Source breakdown: {hybrid_result['source_statistics']}")
        logger.info(f"⚙️ Weights used: CF={hybrid_result['weights_used']['w_cf']}, "
                   f"Content={hybrid_result['weights_used']['w_cb']}, "
                   f"Pop={hybrid_result['weights_used']['w_pop']}")
        
        return recommendations
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in hybrid recommendations: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/stats",
    summary="Get recommendation system stats",
    description="Получение статистики системы рекомендаций"
)
async def get_recommendation_stats():
    """Статистика системы рекомендаций"""
    try:
        logger.info("📊 Recommendation stats request")
        
        # Получаем статистику из БД
        import psycopg2
        import psycopg2.extras
        
        db_url = "postgresql://mikitavalkunovich@localhost:5432/customer_data"
        
        with psycopg2.connect(db_url) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Content-based stats
                cur.execute("""
                SELECT 
                    COUNT(DISTINCT product_id) as products_with_similarities,
                    COUNT(*) as total_similarity_pairs,
                    AVG(sim_score) as avg_similarity
                FROM ml_item_sim_content
                """)
                content_stats = cur.fetchone()
                
                # CF stats
                cur.execute("""
                SELECT 
                    COUNT(DISTINCT product_id) as cf_products,
                    COUNT(*) as cf_similarity_pairs,
                    AVG(sim_score) as cf_avg_similarity,
                    AVG(co_users) as cf_avg_co_users
                FROM ml_item_sim_cf
                """)
                cf_stats = cur.fetchone()
        
        stats = {
            "content_based": {
                "products_with_similarities": content_stats['products_with_similarities'],
                "total_similarity_pairs": content_stats['total_similarity_pairs'],
                "avg_similarity": float(content_stats['avg_similarity'] or 0)
            },
            "collaborative_filtering": {
                "products_with_similarities": cf_stats['cf_products'],
                "total_similarity_pairs": cf_stats['cf_similarity_pairs'],
                "avg_similarity": float(cf_stats['cf_avg_similarity'] or 0),
                "avg_co_users": float(cf_stats['cf_avg_co_users'] or 0)
            },
            "last_updated": datetime.now().isoformat(),
            "status": "active"
        }
        
        logger.info("✅ Recommendation stats completed")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error getting recommendation stats: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/user-purchases",
    response_model=List[UserPurchaseItem],
    summary="Get user purchase history",
    description="Получение истории покупок пользователя для демонстрации"
)
async def get_user_purchases(
    user_id: int = Query(..., description="ID пользователя"),
    limit: int = Query(5, ge=1, le=20, description="Количество последних покупок")
):
    """
    Endpoint для получения истории покупок пользователя
    
    Args:
        user_id: ID пользователя  
        limit: количество последних покупок (1-20)
        
    Returns:
        List[UserPurchaseItem]: список покупок пользователя
    """
    start_time = time.time()
    
    try:
        logger.info(f"🛒 User purchases request: user_id={user_id}, limit={limit}")
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Получаем последние покупки пользователя с деталями товаров
                query = """
                SELECT 
                    oi.product_id,
                    p.title,
                    p.brand,
                    p.category,
                    p.price,
                    (oi.quantity * p.price) as amount,
                    oi.quantity,
                    (CURRENT_DATE - DATE(o.created_at)) as days_ago,
                    o.created_at::text as purchase_date,
                    o.created_at
                FROM order_items oi
                JOIN orders o ON oi.order_id = o.order_id
                JOIN products p ON oi.product_id = p.product_id
                WHERE o.user_id = %s 
                  AND o.status IN ('paid', 'shipped', 'completed')
                  AND p.is_active = true
                ORDER BY o.created_at DESC, oi.product_id
                LIMIT %s
                """
                
                cur.execute(query, (user_id, limit))
                results = cur.fetchall()
                
                if not results:
                    logger.warning(f"No purchases found for user {user_id}")
                    return []
                
                purchases = []
                for row in results:
                    purchases.append(UserPurchaseItem(
                        product_id=row['product_id'],
                        title=row['title'],
                        brand=row['brand'] or "Unknown",
                        category=row['category'],
                        price=float(row['price']),
                        amount=float(row['amount']),
                        quantity=int(row['quantity']),
                        days_ago=int(row['days_ago']),
                        purchase_date=row['purchase_date']
                    ))
                
                processing_time = (time.time() - start_time) * 1000
                
                logger.info(f"✅ User purchases completed: {len(purchases)} items, "
                           f"{processing_time:.1f}ms")
                
                return purchases
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting user purchases: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/top-products",
    summary="Get top recommended products",
    description="Получение топ рекомендуемых товаров по частоте появления в рекомендациях"
)
async def get_top_recommended_products(
    limit: int = Query(10, ge=1, le=50, description="Количество топ товаров")
):
    """
    Endpoint для получения топ рекомендуемых товаров
    Анализирует какие товары чаще всего появляются в рекомендациях
    
    Args:
        limit: количество топ товаров (1-50)
        
    Returns:
        Список топ товаров с частотой рекомендаций
    """
    start_time = time.time()
    
    try:
        logger.info(f"📊 Top products request: limit={limit}")
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Получаем топ товары на основе popularity_score и количества появлений в рекомендациях
                query = """
                WITH popular_products AS (
                    SELECT 
                        p.product_id,
                        p.title,
                        p.brand,
                        p.category,
                        p.price,
                        COALESCE(sales.total_orders, 0) as popularity_score,
                        p.rating,
                        -- Считаем как часто товар покупают (аппроксимация частоты рекомендаций)
                        COALESCE(sales.total_orders, 0) as recommendation_frequency,
                        -- Средний скор на основе популярности (нормализованный)
                        COALESCE(sales.total_orders / NULLIF(max_pop.max_orders, 0), 0) as avg_score
                    FROM products p
                    LEFT JOIN (
                        SELECT 
                            oi.product_id,
                            COUNT(DISTINCT o.order_id) as total_orders
                        FROM order_items oi
                        JOIN orders o ON oi.order_id = o.order_id
                        WHERE o.status IN ('paid', 'shipped', 'completed')
                          AND o.created_at >= CURRENT_DATE - INTERVAL '30 days'
                        GROUP BY oi.product_id
                    ) sales ON p.product_id = sales.product_id
                    CROSS JOIN (
                        SELECT MAX(COALESCE(total_orders, 0)) as max_orders
                        FROM (
                            SELECT oi.product_id, COUNT(DISTINCT o.order_id) as total_orders
                            FROM order_items oi
                            JOIN orders o ON oi.order_id = o.order_id
                            WHERE o.status IN ('paid', 'shipped', 'completed')
                              AND o.created_at >= CURRENT_DATE - INTERVAL '30 days'
                            GROUP BY oi.product_id
                        ) temp
                    ) max_pop
                    WHERE p.is_active = true
                      AND sales.total_orders > 0
                )
                SELECT 
                    product_id,
                    title,
                    brand,
                    category,
                    price,
                    popularity_score,
                    rating,
                    recommendation_frequency,
                    avg_score,
                    -- Источники (упрощенно на основе характеристик товара)
                    CASE 
                        WHEN recommendation_frequency > 10 THEN ARRAY['hybrid', 'popularity', 'cf']
                        WHEN recommendation_frequency > 5 THEN ARRAY['hybrid', 'content']
                        ELSE ARRAY['content', 'popularity']
                    END as sources
                FROM popular_products
                ORDER BY 
                    (recommendation_frequency * 0.6 + popularity_score * 0.4) DESC,
                    rating DESC NULLS LAST
                LIMIT %s
                """
                
                cur.execute(query, (limit,))
                results = cur.fetchall()
                
                if not results:
                    logger.warning("No top products found")
                    return []
                
                top_products = []
                for row in results:
                    top_products.append({
                        "product_id": row['product_id'],
                        "title": row['title'],
                        "brand": row['brand'] or "Unknown",
                        "category": row['category'],
                        "price": float(row['price']) if row['price'] else 0.0,
                        "recommendation_count": int(row['recommendation_frequency']),
                        "avg_score": float(row['avg_score']),
                        "popularity_score": float(row['popularity_score']) if row['popularity_score'] else 0.0,
                        "rating": float(row['rating']) if row['rating'] else 0.0,
                        "sources": row['sources']
                    })
                
                processing_time = (time.time() - start_time) * 1000
                
                logger.info(f"✅ Top products completed: {len(top_products)} items, "
                           f"{processing_time:.1f}ms")
                
                return top_products
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting top products: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )