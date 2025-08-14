from rec import Recommendations

rec_store = Recommendations()

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

res = rec_store.get(user_id=1049126, k=5)
print(res)

# 5470
