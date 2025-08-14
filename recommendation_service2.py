import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from rec import Recommendations
import time
from typing import Dict, Any

logger = logging.getLogger("uvicorn.error")

# Создаём глобальный объект Recommendations
rec_store = Recommendations()

# Глобальные переменные для статистики
service_stats = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "request_times": [],
    "start_time": time.time(),
    "last_request_time": None
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")

    # Загружаем рекомендации при старте сервиса
    rec_store.load(
        "personal",
        'final_recommendations_feat.parquet',
        columns=["user_id", "item_id", "rank"],
    )
    rec_store.load(
        "default",
        'top_recs.parquet',
        columns=["item_id", "rank"],
    )

    yield

    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")
    # Выводим статистику по рекомендациям перед остановкой
    rec_store.stats()

# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    start_time = time.time()
    service_stats["total_requests"] += 1
    service_stats["last_request_time"] = start_time

    try:
        # Получаем рекомендации из хранилища
        recs = rec_store.get(user_id, k)
        service_stats["successful_requests"] += 1
        return {"recs": recs}
    except Exception as e:
        service_stats["failed_requests"] += 1
        raise e
    finally:
        processing_time = time.time() - start_time
        service_stats["request_times"].append(processing_time)


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Проверка здоровья сервиса
    Возвращает "healthy", если сервис работает нормально, иначе "unhealthy"
    """
    try:
        # Простая проверка - пытаемся получить рекомендации для тестового пользователя
        # (используем k=1 для минимальной нагрузки)
        test_recs = rec_store.get(0, 1)
        return {"status": "healthy"}
    except Exception:
        return {"status": "unhealthy"}


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """
    Возвращает статистику работы сервиса
    """
    stats = {
        "total_requests": service_stats["total_requests"],
        "successful_requests": service_stats["successful_requests"],
        "failed_requests": service_stats["failed_requests"],
        "uptime_seconds": time.time() - service_stats["start_time"],
        "last_request_time": service_stats["last_request_time"],
    }

    # Добавляем среднее время обработки, если есть данные
    if service_stats["request_times"]:
        stats["average_processing_time"] = sum(
            service_stats["request_times"]) / len(service_stats["request_times"])
        stats["min_processing_time"] = min(service_stats["request_times"])
        stats["max_processing_time"] = max(service_stats["request_times"])

    # Добавляем статистику из хранилища рекомендаций, если доступно
    if hasattr(rec_store, 'stats'):
        stats["recommendation_stats"] = rec_store.stats()

    return stats
