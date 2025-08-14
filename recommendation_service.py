# Шаг 1. Шаблон сервиса
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from rec import Recommendations

logger = logging.getLogger("uvicorn.error")

# Создаём глобальный объект Recommendations
rec_store = Recommendations()


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
    # Получаем рекомендации из хранилища
    recs = rec_store.get(user_id, k)

    return {"recs": recs}

# Для запуска сервиса:
# uvicorn recommendation_service:app --reload
