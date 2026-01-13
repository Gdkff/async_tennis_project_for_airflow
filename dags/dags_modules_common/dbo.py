import datetime
import asyncpg
import asyncio
from settings.config import POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

CHUNK_SIZE = 1000
MAX_RETRIES = 5


class DBOperator:
    def __init__(self):
        self.pool = None
        self.__tables_schemas = {}

    async def init_db_pool(self):
        self.pool = await asyncpg.create_pool(user=POSTGRES_USER,
                                              password=POSTGRES_PASSWORD,
                                              database=POSTGRES_DB,
                                              host=POSTGRES_HOST,
                                              port=5432,
                                              min_size=5,
                                              max_size=20,
                                              max_inactive_connection_lifetime=100
                                              )

    async def close_pool(self):
        await self.pool.close()

    async def insert(self, db_name: str, table_name: str, data: dict) -> int:
        columns = ', '.join(data.keys())
        placeholders = ', '.join(f'${i + 1}' for i in range(len(data)))
        values = tuple(data.values())
        insert_query = f"""
            INSERT INTO {db_name}.{table_name} ({columns})
            VALUES ({placeholders})
            RETURNING id
        """
        async with self.pool.acquire() as connection:
            result = await connection.fetchrow(insert_query, *values)
        return result['id']

    async def update(self, db_name: str, table_name: str, id: int, update_dict: dict) -> None:
        set_string = ', '.join([f'{key} = ${i + 1}' for i, key in enumerate(update_dict.keys())])
        values = tuple(list(update_dict.values()) + [datetime.datetime.now(), id])
        update_query = f'''
            UPDATE {db_name}.{table_name}
            SET {set_string}, record_updated_at = ${len(values) - 1}
            WHERE id = ${len(values)};
        '''
        async with self.pool.acquire() as connection:
            await connection.execute(update_query, *values)

    async def __get_table_column_names_and_types(self, schema_name: str, table_name: str) -> dict:
        if (schema_name, table_name) in self.__tables_schemas:
            return self.__tables_schemas[(schema_name, table_name)]
        query = """
        SELECT
            column_name,
            udt_name
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name   = $2;
        """
        for attempt in range(1, 6):
            try:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(query, schema_name, table_name)
                table_schema = {row['column_name']: row['udt_name'] for row in rows}
                self.__tables_schemas[(schema_name, table_name)] = table_schema
                return table_schema
            except (asyncpg.exceptions.ConnectionDoesNotExistError, asyncpg.exceptions.InterfaceError) as e:
                print('Получение названий столбцов и их типов не удалось!!!')
                print(e)
                if attempt == 5:
                    raise
                await asyncio.sleep(0.1)

    async def insert_or_update_many(self, schema: str, table: str, records: list[dict], conflict_fields: list[str],
                                    on_conflict_update: bool = True):
        if not records:
            return

        now = datetime.datetime.now()
        for row in records:
            row.setdefault('record_updated_at', now)
            row.setdefault('record_created_at', now)

        columns_insert = list(records[0].keys())
        columns_insert_str = ', '.join(columns_insert)
        columns_update = [c for c in columns_insert if c not in conflict_fields + ['record_created_at', 'id']]
        columns_update_str = ', '.join(f"{col} = EXCLUDED.{col}" for col in columns_update)
        do_on_conflict = f'DO UPDATE SET {columns_update_str}' if on_conflict_update else 'DO NOTHING'

        placeholders = []
        table_column_names_and_types = await self.__get_table_column_names_and_types(schema, table)
        for idx, col in enumerate(columns_insert, start=1):
            col = col.replace('"', '')
            placeholders.append(f"${idx}::{table_column_names_and_types[col]}[]")

        query_template = f"""
        INSERT INTO {schema}.{table} ({columns_insert_str})
        SELECT *
        FROM UNNEST ({', '.join(placeholders)}) AS t({columns_insert_str})
        ON CONFLICT ({', '.join(conflict_fields)}) {do_on_conflict};
        """
        # print(query_template)
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i:i + CHUNK_SIZE]
            values = [[r[col] for col in columns_insert] for r in chunk]
            arrays = list(zip(*values))
            for attempt in range(MAX_RETRIES):
                try:
                    async with self.pool.acquire() as conn:
                        await conn.execute(query_template, *arrays)
                    break
                except (asyncpg.exceptions.ConnectionDoesNotExistError,
                        asyncpg.exceptions.InterfaceError) as e:
                    if attempt == MAX_RETRIES - 1:
                        raise
                    await asyncio.sleep(0.1)

    async def select(self, db_name: str, table_name: str, select_columns_list: list,
                     where_conditions: dict | None = None) -> list:
        selected_columns = ', '.join(select_columns_list) if select_columns_list else '*'
        query = f"SELECT {selected_columns} FROM {db_name}.{table_name}"
        condition = ""
        condition_values = []
        if where_conditions:
            custom_conditions = []
            keys_to_delete = []
            for key, value in where_conditions.items():
                if value is None or (isinstance(value, str) and value.lower() in ['null', 'is null']):
                    custom_conditions.append(f'{key} is null')
                    keys_to_delete.append(key)
                elif isinstance(value, str) and value.lower() in ['not null', 'is not null']:
                    custom_conditions.append(f'{key} is not null')
                    keys_to_delete.append(key)
                elif isinstance(value, bool):
                    custom_conditions.append(f'{key} is {str(value).lower()}')
            for key in keys_to_delete:
                where_conditions.pop(key)
            condition = ' AND '.join(
                f"{key} = ${i + 1}" for i, key in enumerate(where_conditions.keys())
            )
            conditions_with_null = ' AND '.join(custom_conditions)
            if condition:
                if conditions_with_null:
                    condition = condition + ' AND ' + conditions_with_null
            else:
                condition = conditions_with_null
            condition = ' WHERE ' + condition
            condition_values = list(where_conditions.values())
        query += condition
        async with self.pool.acquire() as connection:
            result = await connection.fetch(query, *condition_values)
        return [dict(record) for record in result]

    async def close_pg_connections(self):
        query = """ SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = 'tennis'
                      AND state = 'idle'
                      AND pid <> pg_backend_pid();
                """
        async with self.pool.acquire() as connection:
            await connection.fetch(query)
