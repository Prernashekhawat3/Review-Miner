import urllib.parse
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access environment variables
postgres_password = os.getenv("POSTGRES_PASSWORD")
redis_password =os.getenv("REDIS_PASSWORD")

environment = {
    "local": {
        "db": "postgresql://postgres:first@localhost:5432/postgres?options=-csearch_path%3Ddsiqapp",
        "redis": "redis://:JustWin12@localhost:6379/0",
        "celery-broker": "redis://:JustWin12@localhost:6379/0"
    },
        "server": {
        "db": f"postgresql://postgres:{postgres_password}@192.168.1.101:5432/postgres?options=-csearch_path%3Ddsiqapp",
        "redis": f"redis://:{redis_password}@192.168.1.101:6379/0",
        "celery-broker": f"redis://:{redis_password}@192.168.1.101:6379/0",
        "celery-backend": f"db+postgresql://postgres:{postgres_password}@192.168.1.101:5432/postgres?options=-csearch_path%3Ddsiqapp"
    }}


   