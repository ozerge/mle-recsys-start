from fastapi import FastAPI


class EventStore:
    def __init__(self, max_events_per_user=10):
        """
        Инициализация хранилища событий.
        max_events_per_user — максимальное количество событий, хранимых для одного пользователя.
        """
        self.events = {}  # Словарь {user_id: [item_id_1, item_id_2, ...]}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие (user_id, item_id).
        """
        user_events = self.events.get(
            user_id, [])  # Получаем текущие события пользователя
        # Обновляем список событий
        self.events[user_id] = [item_id] + \
            user_events[: self.max_events_per_user - 1]

    def get(self, user_id, k):
        """
        Возвращает последние k событий для пользователя.
        """
        user_events = self.events.get(
            user_id, [])[:k]  # Получаем последние k событий
        return user_events


# Создаем глобальный объект EventStore
events_store = EventStore()

# Создаём FastAPI приложение
app = FastAPI(title="events")


@app.post("/put")
async def put(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id.
    """
    events_store.put(user_id, item_id)
    return {"result": "ok"}


@app.post("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id.
    """
    events = events_store.get(user_id, k)
    return {"events": events}
