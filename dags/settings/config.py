import os
import requests
from pathlib import Path
from dotenv import load_dotenv

server_ip = "85.190.243.218"


def load_env():
    ip = requests.get("https://api.ipify.org").text
    if ip == server_ip:
        env_path = ".env"
    else:
        base_dir = Path(__file__).resolve().parents[2]
        env_path = base_dir / ".env.local"
    load_dotenv(dotenv_path=env_path)


load_env()
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
