import os
import requests
from pathlib import Path
from dotenv import load_dotenv


server_ip = "85.190.243.218"


# def get_ip():
#     ip = requests.get("https://api.ipify.org").text
#     return ip
#
#
# print(get_ip())
# if get_ip() == server_ip:
#     env_path = ".env"
# else:
#     BASE_DIR = Path(__file__).resolve().parents[2]
#     env_path = BASE_DIR / ".env.local"
#
# print(env_path)
# if os.path.exists(env_path):
#     print("Файл есть")
# else:
#     print("Файл отсутствует")
#
# load_dotenv(dotenv_path=env_path)

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

POSTGRES_CONNECTION_STRING = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}'
print(POSTGRES_CONNECTION_STRING)

# if __name__ == '__main__':
#     print(POSTGRES_CONNECTION_STRING)
