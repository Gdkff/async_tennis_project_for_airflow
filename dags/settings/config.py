import os
import socket
from pathlib import Path
from dotenv import load_dotenv

local_hostname = "inovis"
server_hostname = "vmi2633586"

hostname = socket.gethostname()
BASE_DIR = Path(__file__).resolve().parents[2]
if hostname.startswith(local_hostname):
    env_path = BASE_DIR / ".env.local"
elif hostname.startswith(server_hostname):
    env_path = BASE_DIR / ".env.server"
else:
    exit(hostname)
load_dotenv(dotenv_path=env_path)

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

POSTGRES_CONNECTION_STRING = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}'


if __name__ == '__main__':
    print(POSTGRES_CONNECTION_STRING)
