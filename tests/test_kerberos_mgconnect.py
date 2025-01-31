import os
import sys
import dotenv
import logging
logger = logging.getLogger(__name__)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from update_metagis_layers.db import DBConnect

# test kerberos authentication and database connection

def test_kerberos(db_connect: ('DBConnect'), db_config: dict):
    db_connect.obtain_kerberos_ticket(db_config['kerbuser'], db_config['kerbpwd'])
    if db_connect.kerberos_ticket:
        assert db_connect.kerberos_ticket, "Kerberos ticket should be obtained"
    else:
        logger.error("Kerberos ticket not obtained")
    return db_connect

def test_db_connect(db_connect: ('DBConnect'), db_config: dict):
    db_connect.connect(db_config['server'], db_config['database'], db_config['mguser'], db_config['mgpwd'])
    assert db_connect.connection, "Connection should be established"
    return db_connect

if __name__ == '__main__':

    # load creds from .env file
    dotenv.load_dotenv('data/creds/mg.env')

    db_config = {
        'server': os.getenv('MG_SERVER'),
        'database': os.getenv('MG_DB'),
        'kerbuser': os.getenv('KERBUSER'),
        'kerbpwd': os.getenv('KERBPWD'),
        'mguser': os.getenv('MG_USER'),
        'mgpwd': os.getenv('MG_PWD')
    }   
    db_connect = DBConnect(db_config)

    db_connect = test_kerberos(db_connect, db_config)
    db_connect = test_db_connect(db_connect, db_config)
    logger.info("Kerberos authentication and database connection successful")