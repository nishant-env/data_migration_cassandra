from configparser import RawConfigParser


config = RawConfigParser()
config.read('.conf.ini')

def getDbDetails(config_key):
    return config['DATABASE_CONNECTION'][config_key]

SOURCE_DB = config['DATABASE']['SOURCE_DB']
DESTINATION_KEYSPACE = config['DATABASE']['DESTINATION_KEYSPACE']