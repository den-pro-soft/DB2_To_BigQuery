import json
import pandas as pd
import ibm_db as db2
import ibm_db_dbi
import configparser

from bigquery import BigQuery


class DB2ToBigQuery:
    """
    This class is to transfer data from DB2 to BigQuery.
    """

    def __init__(self, configuration_path='./config/configuration.ini'):
        self.payload = {}
        self.read_init_data(configuration_path)
        self.ibm_db_conn, self.db2_conn = self.connect_to_db2()

    def read_init_data(self, configuration_path):
        """
        Get initial data from configuration.ini file
        :param configuration_path: The path to configuration.ini file
        """
        config = configparser.ConfigParser()

        config.read(configuration_path)

        self.payload['db2_uid'] = config.get('DB2', 'UID')
        self.payload['db2_host_name'] = config.get('DB2', 'HOSTNAME')
        self.payload['db2_password'] = config.get('DB2', 'PASSWORD')
        self.payload['db2_database'] = config.get('DB2', 'DATABASE')
        self.payload['db2_port'] = config.get('DB2', 'PORT')
        self.payload['db2_security'] = config.get('DB2', 'SECURITY')
        self.payload['db2_protocol'] = config.get('DB2', 'PROTOCOL')
        self.payload['db2_tables'] = config.get('DB2', 'TABLES').split(',')

    def connect_to_db2(self):
        """
        Connect to DB2 and get the instance of the handler.
        :return: Tuple of DB2 connection instances
        """
        db2_conn_info = f"DATABASE={self.payload['db2_database']};" +\
                        f"HOSTNAME={self.payload['db2_host_name']};" +\
                        f"PORT={self.payload['db2_port']};" +\
                        f"PROTOCOL={self.payload['db2_protocol']};" +\
                        f"UID={self.payload['db2_uid']};" +\
                        f"PWD={self.payload['db2_password']};" +\
                        f"Security={self.payload['db2_security']};"
        ibm_db_conn = db2.connect(db2_conn_info, '', '')
        conn = ibm_db_dbi.Connection(ibm_db_conn)
        return ibm_db_conn, conn

    def read_table_data(self, db2_table):
        """
        Read all the records in the given db2_table.
        :param db2_table: The name of the table inside DB2 database
        :return: a List of Tuples of table name and dataframe for the records
        """
        cursor = self.db2_conn.cursor()
        bq_table_name = f"{self.payload['db2_database']}.{db2_table}"
        tb_df = None
        query = f"SELECT * FROM {db2_table};"

        try:
            cursor.execute(query)
            tb_df = pd.DataFrame.from_records(
                cursor.fetchall(), columns=[desc[0].strip().replace('#', '').replace(' ', '_')
                                            for desc in cursor.description])
        except Exception as error:
            print(error)
        cursor.close()
        return bq_table_name, tb_df

    @staticmethod
    def deduplicate_table(big_query, bq_table):
        """
        Remove duplicate records in the given table.
        :param big_query: The instance of BigQuery class
        :param bq_table: Name of the table in BigQuery to deduplicate
        """
        query = f"""
            CREATE OR REPLACE TABLE {bq_table}
            AS
            SELECT
            DISTINCT * 
            FROM {bq_table};
        """
        job = big_query.client.query(query)
        result = job.result()

    def load_from_db2_to_bigquery(self):
        """
        Load all tables from DB2 to BigQuery
        """
        big_query = BigQuery(self.payload)

        big_query.create_dataset_if_not_exists(
            self.payload['db2_database'])

        for table in self.payload['db2_tables']:
            bq_table, table_df = self.read_table_data(table)
            # Load data to BQ
            job = big_query.client.load_table_from_dataframe(table_df, bq_table)
            print(f'Completed populating {bq_table} in BigQuery')
            DB2ToBigQuery.deduplicate_table(big_query, bq_table)
            print(f'Completed deduplication in {bq_table}')


if __name__ == '__main__':
    db2_bigquery = DB2ToBigQuery()
    db2_bigquery.load_from_db2_to_bigquery()
