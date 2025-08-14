import logging as logger
import pandas as pd


class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type, path, **kwargs):
        """Загружает и сортирует рекомендации по rank"""
        logger.info(f"Loading recommendations, type: {type}")
        self._recs[type] = pd.read_parquet(path, **kwargs)

        # Сортировка по возрастанию rank (чем меньше rank, тем выше рекомендация)
        if type == "personal":
            self._recs[type] = (self._recs[type]
                                .sort_values("rank")
                                .set_index("user_id"))
        else:
            self._recs[type] = self._recs[type].sort_values("rank")

        logger.info(f"Loaded and sorted by rank")

    def get(self, user_id: int, k: int = 100):
        """Возвращает топ-k рекомендаций, отсортированных по rank"""
        try:
            # Для персональных рекомендаций (уже отсортированы при загрузке)
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            # Для рекомендаций по умолчанию (уже отсортированы при загрузке)
            recs = self._recs["default"]["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        return recs

    def stats(self):
        logger.info("Recommendations service statistics:")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")
