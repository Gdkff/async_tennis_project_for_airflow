import asyncio
from datetime import datetime
from dags_modules import t24_matches, t24_tournaments, t24_players, dbo


class T24:
    def __init__(self):
        self.DBO = dbo.DBOperator()
        self.T24Tournaments = t24_tournaments.T24Tournaments(self.DBO)
        self.T24Players = t24_players.T24Players(self.DBO)
        self.T24Matches = t24_matches.T24Matches(self.DBO)

    async def __async_init_classes(self):
        self.__pool = await self.DBO.init_db_pool()
        await self.DBO.close_pg_connections()

    async def __async_init_classes_variables(self):
        await self.T24Tournaments.init_async()
        await self.T24Players.init_async()

    async def load_daily_matches(self):
        print('Инициализируем пул соединений и загружаем из базы все id игроков')
        await self.__async_init_classes()
        await self.__async_init_classes_variables()
        print('Загружаем сырые данные с матчами')
        await self.T24Matches.get_daily_match_pages()
        print('Находим новые года турниров')
        new_trn_years = await self.T24Matches.get_new_tournaments_and_years(self.T24Tournaments.all_trn_year_draw_ids)
        print('Загружаем в базу новые года турниров и если нужно сами')
        if new_trn_years:
            await self.T24Tournaments.t24_load_tournaments_and_years(new_trn_years)
            await self.T24Tournaments.init_async()
        print('Загружаем все матчи со вчера и на 7 дней вперед')
        correct_matches, defective_matches = await self.T24Matches.get_matches_from_pages(
            self.T24Tournaments.all_trn_year_draw_ids)
        print('Выделяем новых игроков из матчей')
        new_players = await self.T24Players.get_all_new_players_from_matches(correct_matches, defective_matches)
        print('Подготавливаем список словарей с новыми игроками для загрузки в базу данных')
        new_players_to_db = [{'t24_pl_id': pl_id} for pl_id in new_players]
        print('Загружаем в базу id новых игроков')
        await self.DBO.insert_or_update_many('public', 't24_players', new_players_to_db,
                                             ['t24_pl_id'], on_conflict_update=False)
        print('Загружаем корректные матчи в базу')
        await self.DBO.insert_or_update_many('public', 't24_matches', correct_matches, ['t24_match_id'])
        print('Загружаем дефектные матчи в базу')
        await self.DBO.insert_or_update_many('public', 't24_matches_defective', defective_matches, ['t24_match_id'])
        print('Загружаем информацию по новым игрокам')
        new_players_data_to_db = await self.T24Players.load_players_data_to_db([pl_id for pl_id in new_players])
        print('Загружаем данные по игрокам в базу')
        await self.DBO.insert_or_update_many('public', 't24_players', new_players_data_to_db, ['t24_pl_id'],
                                             on_conflict_update=True)
        print('Закрываем пул соединений с БД')
        await self.DBO.close_pool()

    async def load_matches_pbp_data(self):
        await self.__async_init_classes()
        # Загружаем из базы id матчей с незагруженными pbp
        matches_not_loaded_pbp = await self.DBO.select('public', 't24_matches', ['t24_match_id', 'trn_year_id'],
                                                       {'match_status_short_code': 3, 'final_pbp_data_loaded': None})
        matches_not_loaded_pbp_dict = {match['t24_match_id']: match['trn_year_id'] for match in matches_not_loaded_pbp}
        print(f'{len(matches_not_loaded_pbp_dict)} ended matches without PbP loaded')
        batch_size = self.T24Matches.concurrency
        items = list(matches_not_loaded_pbp_dict.items())
        batches = [dict(items[i:i + batch_size]) for i in range(0, len(items), batch_size)]
        batches_count = len(batches)
        print(f'Разбили на батчи. Всего {batches_count} батчей')
        for batch in batches:
            in_time = datetime.now()
            tasks = [self.T24Matches.pbp_get_match_data(t24_match_id) for t24_match_id in batch.keys()]
            pbp_matches = await asyncio.gather(*tasks)
            pbp_games = [pbp_game for match in pbp_matches if match for pbp_game in match if pbp_game]
            print('PbP data downloaded')
            await self.T24Matches.pbp_put_games_to_db(pbp_games)
            matches_loaded_pbp = {x['t24_match_id'] for x in pbp_games}
            update_matches_pbp = [{'t24_match_id': t24_match_id, 'trn_year_id': trn_year_id,
                                   'final_pbp_data_loaded': True if t24_match_id in matches_loaded_pbp else False}
                                  for t24_match_id, trn_year_id in batch.items()]
            await self.DBO.insert_or_update_many('public', 't24_matches', update_matches_pbp,
                                                 ['t24_match_id'], on_conflict_update=True)
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')
        await self.DBO.close_pool()

    async def load_matches_statistics_data(self):
        await self.__async_init_classes()
        batch_size = self.T24Matches.concurrency
        matches_not_loaded_statistics = await self.DBO.select('public', 't24_matches',
                                                              ['t24_match_id', 'trn_year_id'],
                                                              {'match_status_short_code': 3,
                                                               'final_statistics_loaded': None})
        matches_not_loaded_statistics = {match['t24_match_id']: match['trn_year_id']
                                         for match in matches_not_loaded_statistics}
        print(f'{len(matches_not_loaded_statistics)} ended matches without statistics from db loaded')
        items = list(matches_not_loaded_statistics.items())
        batches = [dict(items[i:i + batch_size]) for i in range(0, len(items), batch_size)]
        batches_count = len(batches)
        print(f'Разбили на батчи. Всего {batches_count} батчей')
        for batch in batches:
            in_time = datetime.now()
            tasks = [self.T24Matches.get_match_statistic_by_match_id(t24_match_id)
                     for t24_match_id in batch]
            sets_statistic = await asyncio.gather(*tasks)
            sets_statistic = [set_stat for inner in sets_statistic if inner for set_stat in inner if set_stat]
            print('Statistics data downloaded')
            await self.DBO.insert_or_update_many('public', 't24_set_statistics', sets_statistic,
                                                 ['t24_match_id', 'team_num', 'set'])
            matches_loaded_statistics = {x['t24_match_id'] for x in sets_statistic}
            update_matches_stat = [{'t24_match_id': t24_match_id, 'trn_year_id': trn_year_id,
                                    'final_statistics_loaded': True if t24_match_id in matches_loaded_statistics
                                    else False}
                                   for t24_match_id, trn_year_id in batch.items()]
            await self.DBO.insert_or_update_many('public', 't24_matches', update_matches_stat,
                                                 ['t24_match_id'])
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')
        print('Statistics data uploaded to db')
        await self.DBO.close_pool()

    async def load_tournaments_results(self):
        print('Инициализируем пул соединений и загружаем из базы все id игроков')
        await self.__async_init_classes()
        await self.__async_init_classes_variables()
        print('Загружаем турниры с незагруженными результатами')
        tournaments = await self.DBO.t24_get_tournaments_years_to_load_results()
        batch_size = self.T24Matches.concurrency
        batches = [tournaments[i:i + batch_size] for i in range(0, len(tournaments), batch_size)]
        batches_count = len(batches)
        print(f'Разбили на батчи. Всего {batches_count} батчей')
        for batch in batches:
            in_time = datetime.now()
            trn_years_ids_all = {x['id'] for x in batch}
            tasks = [self.T24Tournaments.get_tournament_results_match_pages(trn) for trn in batch]
            tournaments_match_pages = await asyncio.gather(*tasks)
            tournaments_match_pages = [x for inner in tournaments_match_pages if inner for x in inner if x]
            correct_matches, defective_matches = await self.T24Matches.get_matches_from_pages(
                self.T24Tournaments.all_trn_year_draw_ids, tournaments_match_pages)
            print('Выделяем новых игроков из матчей')
            new_players = await self.T24Players.get_all_new_players_from_matches(correct_matches, defective_matches)
            print('Подготавливаем список словарей с новыми игроками для загрузки в базу данных')
            new_players_to_db = [{'t24_pl_id': pl_id} for pl_id in new_players]
            print(f'Загружаем в базу id новых {len(new_players_to_db)} игроков')
            await self.DBO.insert_or_update_many('public', 't24_players', new_players_to_db,
                                                 ['t24_pl_id'], on_conflict_update=False)
            print(f'Загружаем {len(correct_matches)} корректных матчей в базу')
            await self.DBO.insert_or_update_many('public', 't24_matches', correct_matches, ['t24_match_id'])
            print(f'Загружаем {len(defective_matches)} дефектных матчей в базу')
            await self.DBO.insert_or_update_many('public', 't24_matches_defective', defective_matches, ['t24_match_id'])
            print('Загружаем информацию по новым игрокам')
            new_players_data_to_db = await self.T24Players.load_players_data_to_db([pl_id for pl_id in new_players])
            print('Загружаем данные по игрокам в базу')
            await self.DBO.insert_or_update_many('public', 't24_players', new_players_data_to_db, ['t24_pl_id'],
                                                 on_conflict_update=True)
            print('Смотрим какие года турниров загрузились, обновляем статус trn_results_loaded '
                  'в таблице t24_tournaments_years')
            trn_years_ids_loaded = {x['trn_year_id'] for x in correct_matches + defective_matches}
            trn_years_ids_loaded = [{'id': trn_year_id,
                                     'trn_results_loaded': True if trn_year_id in trn_years_ids_loaded else False}
                                    for trn_year_id in trn_years_ids_all]
            await self.DBO.insert_or_update_many('public', 't24_tournaments_years', trn_years_ids_loaded,
                                                 ['id'], on_conflict_update=True)
            print(f'{len(trn_years_ids_loaded)} t24_tournaments_years.t24_results_loaded updated.')
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')
        print('Закрываем пул соединений с БД')
        await self.DBO.close_pool()


def t24_load_daily_matches():
    t24 = T24()
    asyncio.run(t24.load_daily_matches())
    asyncio.run(t24.load_matches_pbp_data())
    asyncio.run(t24.load_matches_statistics_data())


def t24_load_matches_pbp_and_statistics():
    t24 = T24()
    asyncio.run(t24.load_matches_pbp_data())
    asyncio.run(t24.load_matches_statistics_data())


def t24_load_tournaments_results():
    t24 = T24()
    asyncio.run(t24.load_tournaments_results())
    asyncio.run(t24.load_matches_pbp_data())
    asyncio.run(t24.load_matches_statistics_data())


if __name__ == '__main__':
    start_time = datetime.now()
    t24_load_daily_matches()
    # t24_load_tournaments_results()
    # t24_load_matches_pbp_and_statistics()

    print('Time length:', datetime.now() - start_time)
