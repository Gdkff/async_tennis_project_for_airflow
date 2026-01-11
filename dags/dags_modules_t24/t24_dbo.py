from dags_modules_common.dbo import DBOperator


class DBOT24(DBOperator):
    def __init__(self):
        super().__init__()

    async def close_pg_connections(self):
        query = """ SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = 'tennis'
                      AND state = 'idle'
                      AND pid <> pg_backend_pid();
                """
        async with self.pool.acquire() as connection:
            await connection.fetch(query)

    async def t24_get_tournaments_urls_to_load_years(self):
        select_query = f""" select  id, 
                                    trn_archive_full_url
                            from public.t24_tournaments
                            where trn_years_loaded is null
                            order by 1
                        """
        async with self.pool.acquire() as connection:
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
        async with self.pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]

    async def t24_get_tournaments_years_to_load_results(self):
        select_query = f""" SELECT ty.id, t.trn_type, t.trn_name, ty.trn_year
                            FROM public.t24_tournaments t
                            JOIN public.t24_tournaments_years ty ON t.id = ty.trn_id 
                            WHERE ty.trn_results_loaded is null
                            ORDER BY 1
                         """
        async with self.pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]

