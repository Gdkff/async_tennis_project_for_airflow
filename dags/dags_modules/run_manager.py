import asyncio
from datetime import datetime
from dags_modules import t24_matches, t24_tournaments, t24_players, dbo

class T24:
    def __init__(self):
        self.DBO = dbo.DBOperator()
        self.__pool = None
        self.T24Tournaments = None
        self.T24Players = None
        self.T24Matches = None

    async def __async_init(self):
        self.__pool = await self.DBO.init_db_pool()
        await self.DBO.close_pg_connections(self.__pool)
        self.T24Tournaments = t24_tournaments.T24Tournaments(self.__pool)
        await self.T24Tournaments.init_async()
        self.T24Players = t24_players.T24Players(self.__pool)
        await self.T24Players.init_async()
        self.T24Matches = t24_matches.T24Matches(self.__pool)

    async def load_daily_matches(self):
        print('Инициализируем пул соединений и загружаем из базы все id игроков')
        await self.__async_init()
        print('Загружаем сырые данные с матчами')
        await self.T24Matches.get_daily_match_pages()
        print('Находим новые года турниров')
        new_trn_years = await self.T24Matches.get_new_tournaments_and_years(self.T24Tournaments.all_trn_year_draw_ids)
        print('Загружаем в базу новые года турниров и если нужно сами')
        await self.T24Tournaments.t24_load_tournaments_and_years(new_trn_years)
        await self.T24Tournaments.init_async()
        print('Загружаем все матчи со вчера и на 7 дней вперед')
        correct_matches, defective_matches = await self.T24Matches.get_daily_matches(self.T24Tournaments.all_trn_year_draw_ids)
        print('Выделяем новых игроков из матчей')
        new_players = await self.T24Players.get_all_new_players_from_matches(correct_matches)
        print('Подготавливаем список словарей с новыми игроками для загрузки в базу данных')
        new_players_to_db = [{'t24_pl_id': pl_id} for pl_id in new_players]
        print('Загружаем в базу id новых игроков')
        await self.DBO.insert_or_update_many(self.__pool, 'public', 't24_players', new_players_to_db,
                                             ['t24_pl_id'], on_conflict_update=False)
        print('Загружаем корректные матчи в базу')
        await self.DBO.insert_or_update_many(self.__pool, 'public', 't24_matches', correct_matches, ['t24_match_id'])
        print('Загружаем дефектные матчи в базу')
        await self.DBO.insert_or_update_many(self.__pool, 'public', 't24_matches_defective', defective_matches, ['t24_match_id'])
        print('Загружаем информацию по новым игрокам')
        new_players_data_to_db = await self.T24Players.load_players_data_to_db([pl_id for pl_id in new_players])
        print('Загружаем данные по игрокам в базу')
        await self.DBO.insert_or_update_many(self.__pool, 'public', 't24_players', new_players_data_to_db, ['t24_pl_id'], on_conflict_update=True)
        print('Закрываем пул соединений с БД')
        await self.DBO.close_pool(self.__pool)

def t24_load_daily_matches():
        t24 = T24()
        asyncio.run(t24.load_daily_matches())

if __name__ == '__main__':
    start_time = datetime.now()
    t24_load_daily_matches()
    # t24_load_initial_match_data()
    # t24_load_final_match_data()

    print('Time length:', datetime.now() - start_time)
