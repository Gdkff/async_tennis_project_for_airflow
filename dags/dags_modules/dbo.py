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
            SET {set_string}, record_updated_at = ${len(values) - 1}
            WHERE id = ${len(values)};
        '''
        async with self._pool.acquire() as connection:
            await connection.execute(update_query, *values)

    async def insert_or_update_many(self, schema: str, table: str, records: [dict],
                                    conflict_fields: [str], on_conflict_update: bool = True):
        if not records:
            return
        for row in records:
            if 'record_updated_at' not in row:
                row.update({'record_updated_at': datetime.datetime.now()})
            if 'record_created_at' not in row:
                row.update({'record_created_at': datetime.datetime.now()})
        conflict_fields_str = ', '.join(conflict_fields)
        columns_insert = list(records[0].keys())
        columns_insert_str = ', '.join(columns_insert)
        columns_update = [col for col in list(records[0].keys()) if col not in conflict_fields + ['record_created_at', 'id']]
        columns_update_str = ', '.join(f"{col} = EXCLUDED.{col}" for col in columns_update)
        placeholders = ', '.join(f"${i}" for i in range(1, len(columns_insert) + 1))
        update_set = f"{columns_update_str}"

        if on_conflict_update:
            do_on_conflict = f'UPDATE SET {update_set}'
        else:
            do_on_conflict = 'NOTHING'

        query = f"""
           INSERT INTO {schema}.{table} ({columns_insert_str})
           VALUES ({placeholders})
           ON CONFLICT ({conflict_fields_str}) DO {do_on_conflict}
           """
        values = [tuple([record[col] for col in columns_insert]) for record in records]

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
        condition = ""
        condition_values = []
        if where_conditions:
            conditions_with_null = []
            keys_to_delete = []
            for key, value in where_conditions.items():
                if value is None or value.lower() in ['null', 'is null']:
                    conditions_with_null += [f'{key} is null']
                    keys_to_delete.append(key)
                elif value.lower() in ['not null', 'is not null']:
                    conditions_with_null += [f'{key} is not null']
            for key in keys_to_delete:
                where_conditions.pop(key)
            condition = ' AND '.join(
                f"{key} = ${i + 1}" for i, key in enumerate(where_conditions.keys())
            )
            conditions_with_null = ' AND '.join(conditions_with_null)
            if condition:
                if conditions_with_null:
                    condition = condition + ' AND ' + conditions_with_null
            else:
                condition = conditions_with_null
            condition = ' WHERE ' + condition
            condition_values = list(where_conditions.values())
            print(condition_values)
        query += condition
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
                                    trn_archive_full_url
                            from public.t24_tournaments
                            where trn_years_loaded is null
                            order by 1
                        """
        async with self._pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]

    async def t24_get_tournaments_years_to_load_draw_ids(self):
        select_query = f""" select 	ty.id, 
                                    ty.trn_id,
                                    ty.trn_year,
                                    'https://www.tennis24.com/' || t.trn_type || '/' || t.trn_name || '-' || ty.trn_year as trn_year_url
                            from public.t24_tournaments_years ty
                            left join public.t24_tournaments t on ty.trn_id = t.id
                            where draws_id_loaded is null
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

