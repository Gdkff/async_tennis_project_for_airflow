import os
import socket
from pathlib import Path
from dotenv import load_dotenv


server_ip = "85.190.243.218"


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


print(get_ip())
BASE_DIR = Path(__file__).resolve().parents[2]
if get_ip() == server_ip:
    env_path = BASE_DIR / ".env"
else:
    env_path = BASE_DIR / ".env.local"

load_dotenv(dotenv_path=env_path)

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

POSTGRES_CONNECTION_STRING = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}'
print(POSTGRES_CONNECTION_STRING)

# if __name__ == '__main__':
#     print(POSTGRES_CONNECTION_STRING)
