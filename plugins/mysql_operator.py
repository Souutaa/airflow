from airflow.providers.mysql.hooks.mysql import MySqlHook
from support_processing import TemplateOperatorDB
import logging
from contextlib import closing
import os
import pandas as pd

logger = logging.getLogger(__name__)


class MySQLOperators:
    def __init__(self, conn_id='mysql'):
        try:
            self.mysqlhook = MySqlHook(mysql_conn_id=conn_id)
            self.mysql_conn = self.mysqlhook.get_conn()
        except Exception:
            logger.exception("Can't connect to %s database", conn_id)
            raise

    def get_data_to_pd(self, query=None):
        return self.mysqlhook.get_df(query, df_type='pandas')

    def get_records(self, query):
        return self.mysqlhook.get_records(query)

    def execute_query(self, query):
        try:
            cur = self.mysql_conn.cursor()
            cur.execute(query)
            self.mysql_conn.commit()
        except Exception:
            logger.exception('Cannot execute query')
            raise

    def insert_dataframe_into_table(self, table_name, dataframe, data, chunk_size=100000):
        # if create_table_like != "":
        #     create_tbl_query = f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {create_table_like};"
        #     conn = self.mysql_conn
        #     cur = conn.cursor()
        #     cur.execute(create_tbl_query)
        #     conn.commit()

        query = TemplateOperatorDB(table_name).create_query_insert_into(dataframe)
        try:
            with closing(self.mysql_conn) as conn:
                if self.mysqlhook.supports_autocommit:
                    self.mysqlhook.set_autocommit(conn, False)
                conn.commit()
                with closing(conn.cursor()) as cur:
                    for i in range(0, len(data), chunk_size):
                        partitioned_data = data[i : i + chunk_size]
                        lst = []
                        for row in partitioned_data:
                            sub_lst = []
                            for cell in row:
                                sub_lst.append(self.mysqlhook._serialize_cell(cell, conn))
                            lst.append(sub_lst)
                        values = tuple(lst)
                        num_records = len(values)
                        cur.executemany(query, values)
                        logger.info('Merged or updated %s records', num_records)
                        conn.commit()
                conn.commit()

        except Exception:
            logger.exception('Cannot execute dataframe insert into %s', table_name)
            raise

    def delete_records_in_table(self, table_name, key_field, values):
        query = TemplateOperatorDB(table_name).create_delete_query(key_field, values)

        try:
            with closing(self.mysql_conn) as conn:
                if self.mysqlhook.supports_autocommit:
                    self.mysqlhook.set_autocommit(conn, False)
                conn.commit()
                with closing(conn.cursor()) as cur:
                    lst = []
                    for cell in values:
                        lst.append(self.mysqlhook._serialize_cell(cell, conn))
                    del_values = tuple(lst)
                    cur.execute(query, del_values)
                    num_records = len(values)
                    logger.info('Deleted %s records', num_records)
                    conn.commit()
                conn.commit()
        except Exception:
            logger.exception('Cannot delete records from %s', table_name)
            raise

    def insert_data_into_table(self, table_name, data, create_table_like=''):
        if create_table_like != '':
            create_tbl_query = f'CREATE TABLE IF NOT EXISTS {table_name} LIKE {create_table_like};'
            conn = self.mysql_conn
            cur = conn.cursor()
            cur.execute(create_tbl_query)
            conn.commit()
        try:
            self.mysqlhook.insert_rows(table_name, data)
        except Exception:
            logger.exception('Cannot insert data into %s', table_name)
            raise

    def remove_table_if_exists(self, table_name):
        try:
            remove_table = f'DROP TABLE IF EXISTS {table_name};'
            cur = self.mysql_conn.cursor()
            cur.execute(remove_table)
            self.mysql_conn.commit()
        except Exception:
            logger.exception('Cannot remove table %s', table_name)
            raise

    def truncate_all_data_from_table(self, table_name):
        try:
            truncate_table = f'TRUNCATE TABLE {table_name};'
            cur = self.mysql_conn.cursor()
            cur.execute(truncate_table)
            self.mysql_conn.commit()
        except Exception:
            logger.exception('Cannot truncate table %s', table_name)
            raise

    def dump_table_into_path(self, table_name):
        try:
            priv = self.mysqlhook.get_first('SELECT @@global.secure_file_priv')
            if priv and priv[0]:
                tbl_name = list(table_name)[0]
                file_name = tbl_name.replace('.', '__')
                self.mysqlhook.bulk_dump(tbl_name, os.path.join(priv[0], f'{file_name}.txt'))
            else:
                raise RuntimeError('MySQL secure_file_priv is not configured')
        except Exception:
            logger.exception('Cannot dump %s', table_name)
            raise

    def load_data_into_table(self, table_name):
        try:
            priv = self.mysqlhook.get_first('SELECT @@global.secure_file_priv')
            if priv and priv[0]:
                file_path = os.path.join(priv[0], 'TABLES.txt')
                load_data_into_tbl = f"LOAD DATA INFILE '{file_path}' INTO TABLE {table_name};"
                cur = self.mysql_conn.cursor()
                cur.execute(load_data_into_tbl)
                self.mysql_conn.commit()
            else:
                raise RuntimeError('MySQL secure_file_priv is not configured')
        except Exception:
            logger.exception('Cannot load %s', table_name)
            raise

    def get_large_data_to_postgres(self, query, table_name, postgres_operator, schema='staging', chunk_size=50000):
        """
        Hàm tối ưu: Đọc dữ liệu theo từng khối từ MySQL và ghi trực tiếp cuốn chiếu vào Postgres,
        giúp RAM container không bao giờ bị tràn.
        """

        # Reuse the provider-managed SQLAlchemy engine for MySQL.
        mysql_engine = self.mysqlhook.get_sqlalchemy_engine()

        # Reuse the provider-managed SQLAlchemy engine for PostgreSQL.
        postgres_engine = postgres_operator.hook.get_sqlalchemy_engine()
        # Dùng pandas đọc theo từng khối dữ liệu từ MySQL
        # Tham số chunksize khiến hàm trả về một Generator thay vì nạp cả bảng vào RAM
        first_chunk = True
        for chunk_df in pd.read_sql(query, con=mysql_engine, chunksize=chunk_size):
            # Ghi khối dữ liệu này vào Postgres
            # Nếu là khối đầu tiên (first_chunk=True) -> 'replace' để xóa bảng cũ tạo bảng mới
            # Các khối sau -> 'append' để nối đuôi dữ liệu vào tiếp
            if_exists_mode = 'replace' if first_chunk else 'append'

            chunk_df.to_sql(
                name=table_name,
                con=postgres_engine,
                schema=schema,
                if_exists=if_exists_mode,
                index=False,
                method='multi',  # Tăng tốc độ ghi dữ liệu cho Postgres
            )
            first_chunk = False
            logger.info('Streamed %s rows into %s.%s', len(chunk_df), schema, table_name)
