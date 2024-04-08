from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from .log_util import logger
from config import DESTINATION_KEYSPACE, getDbDetails
from cassandra.query import BatchStatement, ConsistencyLevel

auth_provider = PlainTextAuthProvider(username=f'{getDbDetails("DESTINATION_USER")}', password=f'{getDbDetails("DESTINATION_PASS")}')
cluster = Cluster(['18.140.237.93'], port=9042, auth_provider=auth_provider)

session = cluster.connect(f'{DESTINATION_KEYSPACE}')


def fetch_data_destination(sql_query):
    rows = session.execute('select * from esewa_ns_dev.batch_fcm limit 1')
    return rows


def checkTableExists(table_name):
    query = f"""select 
                    table_name

                    from system_schema.tables 
                    where keyspace_name = '{DESTINATION_KEYSPACE}'
                    and table_name = '{table_name}' """
    result = session.execute(query)
    if not result:
        return 0
    else:
        return 1
    
def createTable(table_name,schema_dict):
    """
    Pass the schema information in the following format:
        Key = Column Name,
        Value = Column Data Type

        also, pass a pair for primary key, where key = 'primary_keys' and value is the list of key columns
    """
    create_query = f'create table {table_name} ( '
    for key, value in schema_dict.items():
        if key != 'primary_keys':
            create_query = create_query + ' ' + key + ' ' + value + ','
    primary_k = '('
    for items in schema_dict['primary_keys']:
        primary_k = primary_k + items
    primary_k = primary_k + ')'

    create_query = create_query + ' primary key ' + primary_k + ');'
    logger.debug(create_query)
    logger.info(f'Creating table, {table_name}')
    session.execute(create_query)


def batchInsert(table_name,columns,rows):
    prepare_statement = f"INSERT INTO {table_name}{columns} values {tuple(['?' for x in range(0,len(columns))])}".replace("'","")
    # print(prepare_statement)
    insert_statement = session.prepare(prepare_statement)
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    # print(columns)
    for row in rows:
        batch.add(insert_statement, row)
    
    session.execute(batch)
    