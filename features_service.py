import logging
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI

logger = logging.getLogger("uvicorn.error")


class SimilarItems:
    def __init__(self):
        self._similar_items = None

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """
        logger.info("Loading data...")
        self._similar_items = pd.read_parquet(path, **kwargs)  # Загружаем файл
        self._similar_items = self._similar_items.set_index(
            "item_id_1")  # Устанавливаем индекс
        logger.info("Loaded")

    def get(self, item_id: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["item_id_2", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error(f"No recommendations found for item_id {item_id}")
            i2i = {"item_id_2": [], "score": []}

        return i2i


# Создаём глобальное хранилище похожих объектов
sim_items_store = SimilarItems()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Функция жизненного цикла FastAPI.
    Загружает данные при старте и освобождает ресурсы при остановке.
    """
    sim_items_store.load(
        "similar_items.parquet",  # Загружаем похожие объекты
        columns=["item_id_1", "item_id_2", "score"],
    )
    logger.info("Feature Store готов к работе!")
    yield  # Здесь сервис работает
    logger.info("Feature Store остановлен.")

# Создаём FastAPI приложение
app = FastAPI(title="features", lifespan=lifespan)


@app.post("/similar_items")
async def recommendations(item_id: int, k: int = 10):
    """
    Возвращает список похожих объектов длиной k для item_id
    """
    i2i = sim_items_store.get(item_id, k)
    return i2i
