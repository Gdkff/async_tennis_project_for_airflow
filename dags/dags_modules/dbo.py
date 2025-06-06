import datetime
import asyncpg
from settings.config import POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB


class DBOperator:
    def __init__(self):
        self._pool = None

    async def init_pool(self):
        self._pool = await asyncpg.create_pool(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
            host=POSTGRES_HOST,
            port=5432,
            min_size=1,
            max_size=5
        )

    async def close_pool(self):
        await self._pool.close()

    async def insert(self, db_name: str, table_name: str, data: dict) -> int:
        columns = ', '.join(data.keys())
        placeholders = ', '.join(f'${i + 1}' for i in range(len(data)))
        values = tuple(data.values())
        insert_query = f"""
            INSERT INTO {db_name}.{table_name} ({columns})
            VALUES ({placeholders})
            RETURNING id
        """
        async with self._pool.acquire() as connection:
            result = await connection.fetchrow(insert_query, *values)
        return result['id']

    async def update(self, db_name: str, table_name: str, id: int, update_dict: dict) -> None:
        set_string = ', '.join([f'{key} = ${i + 1}' for i, key in enumerate(update_dict.keys())])
        values = tuple(list(update_dict.values()) + [datetime.datetime.now(), id])
        update_query = f'''
            UPDATE {db_name}.{table_name}
            SET {set_string}, record_updated = ${len(values) - 1}
            WHERE id = ${len(values)};
        '''
        async with self._pool.acquire() as connection:
            await connection.execute(update_query, *values)

    async def insert_or_update_many(self, schema: str, table: str, records: [dict],
                                    conflict_fields: [str], on_conflict_update: bool = True):
        if not records:
            return
        conflict_fields = ', '.join(conflict_fields)
        columns = ', '.join(records[0].keys())
        placeholders = ', '.join(f"${i}" for i in range(1, len(records[0].keys()) + 1))
        update_set = ', '.join(f"{col} = EXCLUDED.{col}" for col in records[0].keys() if col not in conflict_fields)
        if update_set:
            update_set += f', record_updated = ${len(records[0].keys()) + 1}'
        else:
            update_set += f'record_updated = ${len(records[0].keys()) + 1}'
        if on_conflict_update:
            do_on_conflict = f'UPDATE SET {update_set};'
        else:
            do_on_conflict = 'NOTHING;'
        query = f"""
           INSERT INTO {schema}.{table} ({columns})
           VALUES ({placeholders})
           ON CONFLICT ({conflict_fields}) DO {do_on_conflict}
           """
        if on_conflict_update:
            values = [tuple([record[col] for col in record] + [datetime.datetime.now()]) for record in records]
        else:
            values = [tuple([record[col] for col in record]) for record in records]
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                try:
                    await connection.executemany(query, values)
                except Exception as e:
                    print(query, values)
                    print(e)
                    exit(1)

    async def select(self, db_name: str, table_name: str, select_columns: [str],
                     where_conditions: dict | None = None) -> [dict]:
        selected_columns = ', '.join(select_columns) if select_columns else '*'
        query = f"SELECT {selected_columns} FROM {db_name}.{table_name}"
        condition_clause = ""
        condition_values = []
        if where_conditions:
            condition_clause = ' WHERE ' + ' AND '.join(
                f"{key} = ${i + 1}" for i, key in enumerate(where_conditions.keys())
            )
            condition_values = list(where_conditions.values())
        query += condition_clause
        async with self._pool.acquire() as connection:
            result = await connection.fetch(query, *condition_values)
        return [dict(record) for record in result]
