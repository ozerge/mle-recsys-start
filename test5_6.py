import requests

# URL сервисов
recommendations_url = "http://127.0.0.1:8000"
events_store_url = "http://127.0.0.1:8020"

# Заголовки запроса
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

# 1. Добавляем несколько событий для пользователя 1291248
user_id = 1291248
event_item_ids = [41899, 102868, 5472, 5907]

for event_item_id in event_item_ids:
    requests.post(events_store_url + "/put", headers=headers,
                  params={"user_id": user_id, "item_id": event_item_id})

# 2. Получаем онлайн-рекомендации на основе 3 последних событий
params = {"user_id": user_id, "k": 5}
resp = requests.post(recommendations_url +
                     "/recommendations_online", headers=headers, params=params)

# 3. Выводим результат
online_recs = resp.json()
print(online_recs)

# Сохраняем в файл
with open("test_online_recommendations_result.txt", "w") as f:
    f.write(str(online_recs))
