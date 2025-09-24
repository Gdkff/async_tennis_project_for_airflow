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

        conflict_fields_list = conflict_fields
        conflict_fields_str = ', '.join(conflict_fields_list)
        columns = list(records[0].keys())
        columns_str = ', '.join(columns)
        placeholders = ', '.join(f"${i}" for i in range(1, len(columns) + 1))

        update_fields = [col for col in columns if col not in conflict_fields_list]
        update_set = ', '.join(f"{col} = EXCLUDED.{col}" for col in update_fields)

        update_set = f"{update_set}, record_updated = ${len(columns) + 1}" if update_set else f"record_updated = ${len(columns) + 1}"

        if on_conflict_update:
            do_on_conflict = f'UPDATE SET {update_set}'
        else:
            do_on_conflict = 'NOTHING'

        query = f"""
           INSERT INTO {schema}.{table} ({columns_str})
           VALUES ({placeholders})
           ON CONFLICT ({conflict_fields_str}) DO {do_on_conflict}
           """
        if on_conflict_update:
            values = [tuple([record[col] for col in columns] + [datetime.datetime.now()]) for record in records]
        else:
            values = [tuple([record[col] for col in columns]) for record in records]

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                try:
                    await connection.executemany(query, values)
                except Exception as e:
                    print(query, values)
                    print(e)
                    exit(1)

    async def select(self, db_name: str, table_name: str, select_columns_list: list,
                     where_conditions: dict | None = None) -> [dict]:
        selected_columns = ', '.join(select_columns_list) if select_columns_list else '*'
        query = f"SELECT {selected_columns} FROM {db_name}.{table_name}"
        condition_clause = ""
        condition_values = []
        if where_conditions:
            condition_clause = ' WHERE ' + ' AND '.join(
                f"{key} = ${i + 1}" for i, key in enumerate(where_conditions.keys())
            )
            condition_values = list(where_conditions.values())
        query += condition_clause
        print(query)
        async with self._pool.acquire() as connection:
            result = await connection.fetch(query, *condition_values)
        return [dict(record) for record in result]

    async def t24_get_matches_without_initial_load(self):
        select_query = f"""SELECT t24_match_id, match_url, t1_pl1_name, t1_pl2_name, t2_pl1_name, t2_pl2_name
                           FROM public.t24_matches
                           WHERE initial_players_data_loaded is null
                        """
        async with self._pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]

    async def close_pg_connections(self):
        query = """ SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = 'tennis'
                      AND state = 'idle'
                      AND pid <> pg_backend_pid();
                """
        async with self._pool.acquire() as connection:
            await connection.fetch(query)

    async def t24_get_ended_matches_non_loaded_pbp(self):
        select_query = """  select t24_match_id
                            from public.t24_matches
                            where match_status_short_code = 3 
                              and final_pbp_data_loaded is null
                       """
        async with self._pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]

    async def t24_get_ended_matches_non_loaded_statistics(self):
        select_query = """  select t24_match_id
                            from public.t24_matches
                            where match_status_short_code = 3 
                              and final_statistics_loaded is null
                       """
        async with self._pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]

    async def t24_get_tournaments_urls_to_load_years(self):
        select_query = f""" select  id, 
                                    t24_trn_archive_full_url
                            from public.t24_tournaments
                            where t24_trn_years_loaded is null
                            order by 1
                        """
        async with self._pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]

    async def t24_get_tournaments_years_to_load_results(self):
        select_query = f""" SELECT ty.id, t.t24_trn_type, t.t24_trn_name, ty.t24_trn_year
                            FROM public.t24_tournaments t
                            JOIN public.t24_tournaments_years ty ON t.id = ty.t24_trn_id 
                            WHERE ty.t24_results_loaded is null
                            ORDER BY 1
                         """
        async with self._pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]

