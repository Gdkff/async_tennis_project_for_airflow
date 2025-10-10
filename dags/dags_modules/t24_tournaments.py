import asyncpg

from dags_modules.t24_init import Tennis24, asyncio
from datetime import datetime
import json


class T24Tournaments(Tennis24):
    def __init__(self, pool: asyncpg.pool.Pool):
        super().__init__()
        self.__pool = pool
        self.__all_tournament_urls = dict()
        self.__last_tournament_id = int()
        self.__all_tournament_years = dict()
        self.__tournament_years_with_draw_id = dict()
        self.all_trn_year_draw_ids = dict()
        self.__last_tournaments_years_id = int()
        
    async def init_async(self):
        await self.__get_all_db_tournaments()
        await self.__get_all_db_tournaments_years()

    async def __get_all_db_tournaments(self):
        tournaments = await self._dbo.select(self.__pool, 'public', 't24_tournaments', ['id', 'trn_archive_full_url'])
        self.__all_tournament_urls = {trn['trn_archive_full_url']: trn['id'] for trn in tournaments}
        if tournaments:
            self.__last_tournament_id = max(tournaments, key=lambda x: x['id'])['id']

    async def __get_all_db_tournaments_years(self):
        tournaments_years = await self._dbo.select(self.__pool, 'public', 't24_tournaments_years', ['id', 'trn_id', 'trn_year', 'first_draw_id'])
        self.__all_tournament_years = {(ty['trn_id'], ty['trn_year']): ty['id'] for ty in tournaments_years}
        self.__tournament_years_with_draw_id = {(ty['trn_id'], ty['trn_year']): ty['id'] for ty in tournaments_years if ty['first_draw_id'] is not None}
        self.all_trn_year_draw_ids = {t['first_draw_id']: t['id'] for t in tournaments_years if t['first_draw_id']}
        if tournaments_years:
            self.__last_tournaments_years_id = max(tournaments_years, key=lambda x: x['id'])['id']

    async def __get_tournaments_links_by_trn_type_num(self, trn_type_num: int):
        tournaments_out = []
        tournaments_raw = self._get_html_async(f'https://www.tennis24.com/x/req/m_2_{trn_type_num}', need_soup=False)
        tournaments_split = (await tournaments_raw).split('¬~')
        trn_type = None
        for line in tournaments_split:
            if line.strip() == '':
                continue
            line_split = line.split('¬')
            line_dict = {}
            for line_part in line_split:
                key, value = line_part.split('÷')
                line_dict[key] = value
            if line[:2] == 'MC':
                trn_type = line_dict['ML']
            if line[:2] == 'MN':
                trn_archive_full_url = f'https://www.tennis24.com/{trn_type}/{line_dict['MU']}/archive/'
                if trn_archive_full_url not in self.__all_tournament_urls:
                    current_tournament_id = self.__last_tournament_id + 1
                    tournaments_out.append(
                        {'id': current_tournament_id,
                         'trn_archive_full_url': trn_archive_full_url,
                         'trn_type': trn_type,
                         'trn_name': line_dict['MU']
                         })
                    self.__last_tournament_id = current_tournament_id
                    self.__all_tournament_urls.add(trn_archive_full_url)
        return tournaments_out

    async def __get_tournament_years(self, tournament_data: dict) -> dict | None:
        soup = await self._get_html_async(tournament_data['trn_archive_full_url'], need_soup=True)
        tournament_data['years'] = []
        if soup:
            archive_years = soup.find_all('div', class_='archive__row')
            for trn_raw in archive_years:
                trn_name_with_year = trn_raw.find('div', class_='archive__season').text.strip()
                year = trn_name_with_year.split()[-1]
                if not year.isdigit():
                    print(f'!!! Year: {year}')
                    continue
                year = int(year)
                tournament_data['years'].append(year)
        return tournament_data

    async def __get_trn_draws_id(self, trn: dict):
        first, qual, main = None, None, None
        html = await self._get_html_async(trn['trn_year_url'], need_soup=False)
        if not html:
            return None
        start_first = html.find('{"tournament":"') + 15
        end_first = start_first + 8
        if start_first > 15:
            first = html[start_first:end_first]
        start_second = html.find('"stages":[{"id')
        end_second = html.find('}]', start_second) + 2
        stages_str = '{' + html[start_second:end_second] + '}'
        stages = json.loads(stages_str)
        if stages == {}:
            return {'id': trn['id'],
                    'trn_id': trn['trn_id'],
                    'trn_year': trn['trn_year'],
                    'first_draw_id': None,
                    'qual_draw_id': None,
                    'main_draw_id': None,
                    'draws_id_loaded': False}
        for stage in stages.get('stages'):
            if 'Qualification' in stage.get('name'):
                qual = stage.get('id')
            elif 'Main' in stage.get('name'):
                main = stage.get('id')
        return {'id': trn['id'],
                'trn_id': trn['trn_id'],
                'trn_year': trn['trn_year'],
                'first_draw_id': first,
                'qual_draw_id': qual,
                'main_draw_id': main,
                'draws_id_loaded': True if first and (qual or main) else False}

    async def load_tournaments(self, tournaments_to_load: list[dict] | None = None, on_conflict_update: bool | None = None):
        on_conflict_update = on_conflict_update if on_conflict_update else False
        if not tournaments_to_load:
            tasks = [self.__get_tournaments_links_by_trn_type_num(trn_type_num)
                     for trn_type_num in list(range(5724, 5743)) + [6393, 7897, 7898, 7899, 7900, 8430, 10883]]
            tournaments = await asyncio.gather(*tasks)
            tournaments = [trn for inner in tournaments if inner for trn in inner if trn]
        else:
            tournaments = list()
            for trn in tournaments_to_load:
                if trn['trn_archive_full_url'] not in self.__all_tournament_urls:
                    self.__last_tournament_id += 1
                    trn_id = self.__last_tournament_id
                else:
                    trn_id = self.__all_tournament_urls[trn['trn_archive_full_url']]
                tournaments.append({'id': trn_id,
                                    'trn_archive_full_url': trn['trn_archive_full_url'],
                                    'trn_type': trn['trn_type'],
                                    'trn_name': trn['trn_name'],
                                    'trn_years_loaded': None})
            self.__all_tournament_urls.update({trn['trn_archive_full_url']: trn['id'] for trn in tournaments})
        await self._dbo.insert_or_update_many(self.__pool, 'public', 't24_tournaments', tournaments,
                                              ['trn_archive_full_url'], on_conflict_update=on_conflict_update)

    async def load_tournaments_years(self):
        tournaments_urls = await self._dbo.t24_get_tournaments_urls_to_load_years(self.__pool)
        batch_size = self._concurrency
        batches = [tournaments_urls[i:i + batch_size] for i in range(0, len(tournaments_urls), batch_size)]
        batches_count = len(batches)
        for batch in batches:
            in_time = datetime.now()
            all_trns_in_batch = {trn['id'] for trn in batch}
            tasks = [self.__get_tournament_years(trn) for trn in batch]
            tournaments_with_years = await asyncio.gather(*tasks)
            print(f'{len(tournaments_with_years)} tournaments with years got.')
            trn_years_data_to_db = []
            for trn in tournaments_with_years:
                for year in trn['years']:
                    if (trn['id'], year) not in self.__tournament_years_with_draw_id:
                        trn_year_id = self.__last_tournaments_years_id + 1 if (trn['id'], year) not in self.__all_tournament_years else self.__all_tournament_years[(trn['id'], year)]
                        trn_years_data_to_db.append({'id': trn_year_id,
                                                     'trn_id': trn['id'],
                                                     'trn_year': year,
                                                     'draws_id_loaded': None
                                                     })
                        self.__last_tournaments_years_id += 1
                        # print('current_tournaments_years_id =', self.__last_tournaments_years_id)
                        self.__all_tournament_years.update({(trn['id'], year): self.__last_tournaments_years_id})
            await self._dbo.insert_or_update_many(self.__pool, 'public', 't24_tournaments_years', trn_years_data_to_db,
                                                  ['trn_id', 'trn_year'], on_conflict_update=True)
            print(f'{len(trn_years_data_to_db)} records loaded to t24_tournaments_years')
            trns_with_years = {y['trn_id'] for y in trn_years_data_to_db}
            trns_to_update = [{'id': trn_id, 'trn_years_loaded': True if trn_id in trns_with_years else False}
                              for trn_id in all_trns_in_batch]
            tasks = [self._dbo.update(self.__pool, 'public', 't24_tournaments', trn['id'],
                                      {'trn_years_loaded': trn['trn_years_loaded']})
                     for trn in trns_to_update]
            await asyncio.gather(*tasks)
            print(f'{len(tournaments_with_years)} t24_tournaments.t24_trn_years_loaded updated.')
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')

    async def load_tournaments_draws_id(self):
        tournaments_years = await self._dbo.t24_get_tournaments_years_to_load_draw_ids(self.__pool)
        batch_size = self._concurrency
        batches = [tournaments_years[i:i + batch_size] for i in range(0, len(tournaments_years), batch_size)]
        batches_count = len(batches)
        print(f'Batch size: {batch_size}')
        for batch in batches:
            in_time = datetime.now()
            tasks = [self.__get_trn_draws_id(trn) for trn in batch]
            tournaments_draws_id = await asyncio.gather(*tasks)
            tournaments_draws_id = [trn_draws_id for trn_draws_id in tournaments_draws_id if trn_draws_id]
            await self._dbo.insert_or_update_many(self.__pool, 'public', 't24_tournaments_years', tournaments_draws_id,
                                                  ['trn_id', 'trn_year'], on_conflict_update=True)
            print(f'{len([trn for trn in tournaments_draws_id if trn['draws_id_loaded']])} draws id loaded')
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')

    async def t24_load_tournaments_and_years(self, tournaments_to_load: list[dict] | None = None):
        await self.init_async()
        await self.load_tournaments(tournaments_to_load)
        await self.load_tournaments_years()
        await self.load_tournaments_draws_id()


if __name__ == '__main__':
    t24 = T24Tournaments()
    asyncio.run(t24.t24_load_tournaments_and_years())

