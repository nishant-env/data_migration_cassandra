from utils import checkTableExists,getTableList, logger, getSchemaInfo, createTable, get_rows_count, batchInsert, get_source_data
from config import getDbDetails, SOURCE_DB
from cassandra.query import BatchStatement


### temporarily
batch_size = 100

if __name__ == '__main__':
    table_list = getTableList(f'{SOURCE_DB}')
    for table in table_list:
        logger.info(f'Working for table: {table[0]}')
        do_table_exists = checkTableExists(f'{table[0]}')
        logger.info(f'Checking if table exists: got- {do_table_exists}')
        schema_info = getSchemaInfo(SOURCE_DB, table[0])
        print(schema_info)
        if do_table_exists == 0:
            createTable(table[0], schema_info)
            logger.info('Schema Migration Completed')
        logger.info('Now moving data')


        ## test data move  
        logger.info(f'Started migration for table {table[0]}')
        total_rows_table = get_rows_count(SOURCE_DB, table[0])
        logger.info(f'Got total rows for table {table[0]}: {total_rows_table}')
        columns = tuple([x for x in schema_info.keys() if x != 'primary_keys'])
        ### creating batch
        for lower_limit in range(0, total_rows_table, batch_size):
            rows = get_source_data(SOURCE_DB, table[0], lower_limit, batch_size)
            batchInsert(table[0], columns, rows)
        
    
        

        
        
