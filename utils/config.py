import sys
import os
import logging
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
load_dotenv(os.path.join(SCRIPT_DIR, "../.env"))
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(filename)s - %(levelname)s -: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


postgres_user = os.environ['DB_USER']
postgres_password = os.environ['DB_PASSWORD']
postgres_host = os.environ['DB_HOST']
postgres_db = os.environ['DB_NAME']
postgres_port = os.environ['DB_PORT']


def get_csv_file_path(sub_folder: str):

    fileder_path = os.path.join(base_dir, f"data/{sub_folder}")
    if not os.path.exists(fileder_path):
        os.makedirs(fileder_path)
    return fileder_path
