import logging
import requests
from fastapi import FastAPI
from contextlib import asynccontextmanager
from rec import Recommendations

logger = logging.getLogger("uvicorn.error")

# Адреса сервисов
features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"

# Создаём глобальный объект Recommendations
rec_store = Recommendations()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Функция жизненного цикла FastAPI. Загружает рекомендации при запуске сервиса.
    """
    logger.info("Starting recommendation service...")

    # Загружаем персональные и default-рекомендации
    rec_store.load("personal", "final_recommendations_feat.parquet",
                   columns=["user_id", "item_id", "rank"])
    rec_store.load("default", "top_recs.parquet", columns=["item_id", "rank"])

    logger.info("Ready!")
    yield
    logger.info("Stopping recommendation service")


# Создаём FastAPI приложение
app = FastAPI(title="recommendations", lifespan=lifespan)


def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение.
    """
    seen = set()
    return [id for id in ids if not (id in seen or seen.add(id))]


@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список офлайн-рекомендаций длиной k для пользователя user_id.
    """
    recs = rec_store.get(user_id, k)
    return {"recs": recs}


@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id.
    """
    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # Получаем список последних событий пользователя (3 последних)
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(events_store_url + "/get",
                         headers=headers, params=params)
    events = resp.json().get("events", [])

    # Получаем список похожих объектов
    items = []
    scores = []
    for item_id in events:
        params = {"item_id": item_id, "k": k}
        resp = requests.post(features_store_url +
                             "/similar_items", headers=headers, params=params)
        item_similar_items = resp.json()

        items += item_similar_items["item_id_2"]
        scores += item_similar_items["score"]

    # Сортируем по убыванию score
    combined = sorted(zip(items, scores), key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # Убираем дубликаты
    recs = dedup_ids(combined)

    return {"recs": recs[:k]}


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций, совмещая офлайн- и онлайн-рекомендации.
    """

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []
    min_length = min(len(recs_offline), len(recs_online))

    # Чередуем рекомендации
    for i in range(min_length):
        recs_blended.append(recs_online[i])  # Нечетные места — онлайн
        recs_blended.append(recs_offline[i])  # Четные места — офлайн

    # Добавляем оставшиеся элементы в конец
    recs_blended += recs_online[min_length:] + recs_offline[min_length:]

    # Убираем дубликаты
    recs_blended = dedup_ids(recs_blended)

    # Оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k]

    return {"recs": recs_blended}
