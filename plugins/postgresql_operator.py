from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def get_connection(self):
        return self.hook.get_conn()

    def get_data_to_pd(self, sql):
        return self.hook.get_df(sql, df_type='pandas')

    def save_data_to_postgres(self, df, table_name, schema='public', if_exists='replace'):
        engine = self.hook.get_sqlalchemy_engine()
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi',
        )

    def execute_query(self, sql):
        self.hook.run(sql)
