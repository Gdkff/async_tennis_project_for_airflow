from dags_modules_common.dbo import DBOperator


class DBOATP(DBOperator):
    def __init__(self):
        super().__init__()

    async def atp_get_distinct_dates_from_ranking(self, ranking_type: str):
        select_query = f""" select  distinct week_date
                            from public.atp_ranking_{ranking_type}
                        """
        async with self.pool.acquire() as connection:
            result = await connection.fetch(select_query)
        return [dict(record) for record in result]
