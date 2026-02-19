import asyncio
from datetime import datetime, date
from dags_modules_atp import atp_ranking, atp_dbo, atp_players, atp_init, atp_tournaments, atp_matches


class ATP(atp_init.ATPInit):
    def __init__(self):
        super().__init__()
        self.DBO = atp_dbo.DBOATP()
        self.ATPRanking = atp_ranking.ATPRanking(self.DBO)
        self.ATPPlayers = atp_players.ATPPlayers(self.DBO)
        self.ATPMatches = atp_matches.ATPMatches(self.DBO)
        self.ATPTournaments = atp_tournaments.ATPTournaments(self.DBO)

    async def __async_init_classes(self):
        self.__pool = await self.DBO.init_db_pool()
        await self.DBO.close_pg_connections()

    async def __async_init_classes_variables(self):
        await self.ATPRanking.init_async()
        await self.ATPPlayers.init_async()

    async def load_rankings(self):
        print('Инициализируем пул соединений и загружаем переменные')
        await self.__async_init_classes()
        await self.__async_init_classes_variables()
        batch_size = self.concurrency
        print(f'Батч: {batch_size} дат рейтинга')
        for ranking_type_full in ('singles', 'doubles'):
            ranking_type_short_to_db = 'S' if ranking_type_full == 'singles' else 'D'
            print(f'Загружаем все даты недель рейтинга {ranking_type_full}')
            all_web_dates = await self.ATPRanking.web_ranking_dates_loading(ranking_type_full)
            print('Отбираем новые даты, которые еще не загружены')
            new_ranking_dates = [{'ranking_type': ranking_type_short_to_db,
                                  'week_date': d,
                                  'url': f'https://www.atptour.com/en/rankings/{ranking_type_full}'
                                         f'?dateWeek={d}&rankRange=0-5000'}
                                 for d in all_web_dates if d not in self.ATPRanking.db_dates[ranking_type_full]]
            print(f'Найдено новых дат: {len(new_ranking_dates)}')
            if len(new_ranking_dates) > 0:
                await self.DBO.insert_or_update_many('public', f'atp_ranking_dates', new_ranking_dates,
                                                     ['ranking_type', 'week_date'])
                print('Новые даты записаны в базу')
            print('Загружаем даты с незагруженным рейтингом')
            dates_to_load = await self.DBO.select('public', 'atp_ranking_dates', ['week_date'],
                                                  {'ranking_type': ranking_type_short_to_db, 'loaded': None})
            dates_to_load = [d['week_date'] for d in dates_to_load]
            batches = [dates_to_load[i:i + batch_size] for i in range(0, len(dates_to_load), batch_size)]
            batches_count = len(batches)
            print(f'Разбили на батчи. Всего {batches_count} батчей')
            for batch in batches:
                in_time = datetime.now()
                tasks = [self.ATPRanking.get_ranking_by_date_and_type(ranking_type_full, date_week)
                         for date_week in batch]
                print('Загружаем рейтинг с сайта')
                batch_data = await asyncio.gather(*tasks)
                print('Рейтинг загружен')
                ranking_dates_update = [data['date'] for data in batch_data if data['date']]
                ranking_rows_to_db = []
                for date_ranking in batch_data:
                    ranking_rows_to_db += date_ranking['rank'] if date_ranking['rank'] else []
                ranking_players = {row['atp_pl_id'] for row in ranking_rows_to_db}
                new_players = await self.ATPPlayers.find_real_new_players_in_list(ranking_players)
                players_to_db = [{'atp_pl_id': pl} for pl in new_players]
                print(f'Загружаем в базу {len(players_to_db)} id новых игроков в батче, также добавляем их в кэш')
                await self.DBO.insert_or_update_many('public', 'atp_players', players_to_db, ['atp_pl_id'])
                self.ATPPlayers.all_pl_ids.update(new_players)
                print(f'Загружаем в базу рейтинг {ranking_type_full}')
                await self.DBO.insert_or_update_many('public', f'atp_ranking_{ranking_type_full}', ranking_rows_to_db,
                                                     ['week_date', 'atp_pl_id'])
                print('Обновляем даты рейтинга')
                await self.DBO.insert_or_update_many('public', 'atp_ranking_dates', ranking_dates_update,
                                                     ['ranking_type', 'week_date'])
                batches_count -= 1
                print(f'Батч загружен за {datetime.now() - in_time}. Осталось загрузить {batches_count}')

    async def load_new_players(self):
        await self.__async_init_classes()
        await self.__async_init_classes_variables()
        print('Загружаем id игроков с незагруженными данными')
        not_loaded_players = await self.DBO.select('public', 'atp_players', ['atp_pl_id'],
                                                   {'pl_data_loaded': None})
        not_loaded_players = [d['atp_pl_id'] for d in not_loaded_players]
        print('Id игроков загружены')
        batch_size = self.ATPRanking.concurrency
        batches = [not_loaded_players[i:i + batch_size] for i in range(0, len(not_loaded_players), batch_size)]
        batches_count = len(batches)
        print(f'Разбили на батчи. Всего {batches_count} батчей')
        for batch in batches:
            in_time = datetime.now()
            tasks = [self.ATPPlayers.get_pl_info_by_atp_pl_id(atp_pl_id)
                     for atp_pl_id in batch]
            print('Загружаем данные игроков')
            batch_players_data = await asyncio.gather(*tasks)
            print('Данные игроков в батче загружены')
            await self.DBO.insert_or_update_many('public', 'atp_players', batch_players_data, ['atp_pl_id'])
            print('Id новых игроков в батче загружены в базу')
            batches_count -= 1
            print(f'Батч загружен за {datetime.now() - in_time}. Осталось загрузить {batches_count}')

    async def get_archive_tournaments(self):
        await self.__async_init_classes()
        print('Готовим года к загрузке архивных турниров, бьем на батчи')
        years_list = list(range(1915, datetime.now().year + 1))
        batch_size = self.ATPTournaments.concurrency
        batches = [years_list[i:i + batch_size] for i in range(0, len(years_list), batch_size)]
        batches_count = len(batches)
        print(f'Всего {batches_count} батчей с годами')
        for batch in batches:
            in_time = datetime.now()
            tasks = [self.ATPTournaments.get_one_year_result_archive(year) for year in batch]
            print('Загружаем архивные турниры')
            batch_archive_data = await asyncio.gather(*tasks)
            batch_archive_data = [d for inner in batch_archive_data for d in inner]
            print('Данные в батче архивных турниров с сайта загружены')
            await self.DBO.insert_or_update_many('public', 'atp_tournaments', batch_archive_data,
                                                 ['atp_trn_id', 'trn_year', 'trn_start_date'])
            print('Данные в батче архивных турниров загружены в БД')
            batches_count -= 1
            print(f'Батч загружен за {datetime.now() - in_time}. Осталось загрузить {batches_count}')

    async def load_tournaments_matches(self):
        await self.__async_init_classes()
        await self.__async_init_classes_variables()
        tournaments = await self.DBO.select('public', 'atp_tournaments', [
            'atp_trn_id', 'trn_year', 'tour_type', 'trn_name', 'trn_start_date', 'trn_end_date', 'trn_city',
            'trn_country', 'singles_main_draw_matches', 'singles_qualification_draw_matches',
            'doubles_main_draw_matches', 'doubles_qualification_draw_matches', 'draws_count', 'matches_loaded'],
                                            {'matches_loaded': None})
        batch_size = self.ATPTournaments.concurrency
        # batch_size = 1
        batches = [tournaments[i:i + batch_size] for i in range(0, len(tournaments), batch_size)]
        batches_count = len(batches)
        print(f'Всего {batches_count} батчей')
        for batch in batches:
            in_time = datetime.now()
            tasks = [self.ATPMatches.get_tournament_matches(trn) for trn in batch]
            trn_and_match_data = await asyncio.gather(*tasks)
            players_out_set = set()
            matches_out = []
            tournaments_out = []
            for trn, matches, players in trn_and_match_data:
                tournaments_out.append(trn)
                matches_out.extend(matches)
                players_out_set.update({pl for pl in players if pl not in self.ATPPlayers.all_pl_ids})
            players_out = [{'atp_pl_id': pl} for pl in players_out_set]
            await self.DBO.insert_or_update_many('public', 'atp_players', players_out,
                                                 ['atp_pl_id'])
            self.ATPPlayers.all_pl_ids.update({pl['atp_pl_id'] for pl in players_out})
            await self.DBO.close_pg_connections()
            await self.DBO.insert_or_update_many('public', 'atp_matches', matches_out,
                                                 ['atp_trn_id', 'trn_year', 'trn_start_date', 'draw_name',
                                                  'round_number', 'match_number'])
            await self.DBO.close_pg_connections()
            await self.DBO.insert_or_update_many('public', 'atp_tournaments', tournaments_out,
                                                 ['atp_trn_id', 'trn_year', 'trn_start_date'])
            batches_count -= 1
            print(f'Батч загружен за {datetime.now() - in_time}. Осталось загрузить {batches_count}')


def atp_load_rankings():
    atp = ATP()
    asyncio.run(atp.load_rankings())


def atp_load_new_players():
    atp = ATP()
    asyncio.run(atp.load_new_players())


def get_archive_tournaments():
    atp = ATP()
    asyncio.run(atp.get_archive_tournaments())


def get_tournaments_matches():
    atp = ATP()
    asyncio.run(atp.load_tournaments_matches())


if __name__ == '__main__':
    start_time = datetime.now()
    # atp_load_rankings()
    # atp_load_new_players()
    # get_archive_tournaments()
    get_tournaments_matches()

    print('Time length:', datetime.now() - start_time)
