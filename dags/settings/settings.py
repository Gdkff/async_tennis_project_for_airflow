import socket
import os
from dotenv import load_dotenv

hostname = socket.gethostname()
if hostname.startswith("inovis"):
    env_file = ".env.local"
elif hostname.startswith("vmi2633586"):
    env_file = ".env.server"
else:
    exit(1)
load_dotenv(dotenv_path=env_file, override=True)

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

POSTGRES_CONNECTION_STRING = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}'
