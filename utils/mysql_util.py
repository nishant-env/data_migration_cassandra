from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from config import getDbDetails
from .log_util import logger

engine = create_engine(getDbDetails('SOURCE_DB_CONNECTION'))

replacement_types = {
    'datetime': 'timestamp',
    'varchar' : 'text',
    'bit': 'boolean'
}



def getTableList(source_db):
    query = f"""
        select
            table_name

            from information_schema.tables 
            where table_schema='{source_db}';
        """
    logger.info(f'Getting tables list for schema {source_db}')
    with Session(engine) as session:
        result = session.execute(query).all()

    return result


def getSchemaInfo(source_db,table_name):
    structure_query = f"""
            select
                column_name,
                data_type

                from information_schema.columns 
                where table_schema='{source_db}'
                and table_name = '{table_name}';"""
    primary_key_query = f"""
            select
                GROUP_CONCAT(column_name) AS 'PRIMARY_KEYS'

                from information_schema.columns 
                where table_schema='{source_db}'
                and table_name = '{table_name}'
                and column_key = 'PRI'
                """
    with Session(engine) as session:
        structure = session.execute(structure_query).all()
        primary_keys = session.execute(primary_key_query).all()

    table_structure = {}
    for col_name, d_type in structure:
        if d_type in list(replacement_types.keys()):
            d_type = replacement_types[d_type]
        table_structure[col_name] = d_type
    table_structure['primary_keys'] = [x[0] for x in primary_keys]
    return table_structure

def get_rows_count(table_schema, table_name):
    query = f'select count(1) from {table_schema}.{table_name}'
    with Session(engine) as session:
        result = session.execute(query).all()
    
    return result[0][0]


def get_source_data(table_schema, table_name, limit_min, batch_size):
    query = f'select * from {table_schema}.{table_name} limit {limit_min},{batch_size};'
    logger.debug(f'Query for fetching source data, {query}')
    with Session(engine) as session:
        result = session.execute(query).all()

    return result