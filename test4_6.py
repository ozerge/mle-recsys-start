import requests

# URL сервисов
events_store_url = "http://127.0.0.1:8020"  # Event Store
features_store_url = "http://127.0.0.1:8010"  # Feature Store
recommendations_url = "http://127.0.0.1:8000"  # Recommendation Service

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}


def test_event_store():
    """Тестирование Event Store"""
    # 1. Проверяем, что для пользователя 1127794 нет событий
    user_id = 1127794
    params = {"user_id": user_id}
    resp = requests.post(events_store_url + "/get",
                         headers=headers, params=params)
    print("1. События до добавления:", resp.json())

    # 2. Добавляем события
    items_to_add = [18734992, 18734992, 7785, 4731479]
    for item_id in items_to_add:
        params = {"user_id": user_id, "item_id": item_id}
        resp = requests.post(events_store_url + "/put",
                             headers=headers, params=params)
        print(f"2. Добавлено событие: user {user_id}, item {item_id}")

    # 3. Получаем последние 3 события
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(events_store_url + "/get",
                         headers=headers, params=params)
    print("3. Последние 3 события:", resp.json())


def test_recommendations():
    """Тестирование Recommendation Service"""
    user_id = 1291248

    # 1. Проверяем онлайн-рекомендации до добавления событий
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(
        recommendations_url + "/recommendations_online", headers=headers, params=params)
    print("4. Рекомендации до добавления события:", resp.json())

    # 2. Добавляем событие (item_id = 17245)
    params = {"user_id": user_id, "item_id": 17245}
    requests.post(events_store_url + "/put", headers=headers, params=params)

    # 3. Проверяем онлайн-рекомендации после добавления события
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(
        recommendations_url + "/recommendations_online", headers=headers, params=params)
    print("5. Рекомендации после добавления события:", resp.json())


if __name__ == "__main__":
    print("=== Тестирование Event Store ===")
    test_event_store()

    print("\n=== Тестирование Recommendation Service ===")
    test_recommendations()
