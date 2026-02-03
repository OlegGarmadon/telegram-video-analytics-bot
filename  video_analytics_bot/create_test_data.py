import json
import random
from datetime import datetime, timedelta

# Генерация тестовых данных
videos = []

for video_id in range(1, 51):  # 50 видео
    video_created_at = datetime(2025, 11, 1) + timedelta(days=random.randint(0, 30))
    
    video = {
        "id": video_id,
        "creator_id": random.randint(1, 10),
        "video_created_at": video_created_at.isoformat() + "Z",
        "views_count": random.randint(1000, 1000000),
        "likes_count": random.randint(50, 50000),
        "comments_count": random.randint(0, 10000),
        "reports_count": random.randint(0, 100),
        "created_at": video_created_at.isoformat() + "Z",
        "updated_at": (video_created_at + timedelta(hours=1)).isoformat() + "Z",
        "snapshots": []
    }
    
    # Генерация снапшотов (24 часа)
    for hour in range(0, 24):
        snapshot_time = video_created_at + timedelta(hours=hour)
        snapshot = {
            "id": video_id * 1000 + hour,
            "views_count": video["views_count"] + hour * 100,
            "likes_count": video["likes_count"] + hour * 10,
            "comments_count": video["comments_count"] + hour * 2,
            "reports_count": video["reports_count"],
            "delta_views_count": random.randint(50, 500),
            "delta_likes_count": random.randint(0, 20),
            "delta_comments_count": random.randint(0, 5),
            "delta_reports_count": random.randint(0, 1),
            "created_at": snapshot_time.isoformat() + "Z",
            "updated_at": (snapshot_time + timedelta(minutes=5)).isoformat() + "Z"
        }
        video["snapshots"].append(snapshot)
    
    videos.append(video)

# Сохраняем в файл
with open('videos.json', 'w', encoding='utf-8') as f:
    json.dump(videos, f, ensure_ascii=False, indent=2)

print(f"✅ Создан тестовый файл videos.json с {len(videos)} видео")