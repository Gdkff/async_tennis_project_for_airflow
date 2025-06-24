from dags_modules.t24_init import Tennis24, asyncio
from datetime import datetime, date, timedelta, timezone
from settings.config import tz


class T24InitDataBase(Tennis24):
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
                tournaments_out.append(
                    {'t24_trn_archive_full_url': f'https://www.tennis24.com/{trn_type}/{line_dict['MU']}/archive/',
                     't24_trn_type': trn_type,
                     't24_trn_name': line_dict['MU']})
                # print(f'https://www.tennis24.com/{trn_type}/{line_dict['MU']}/archive/')
        return tournaments_out

    async def __get_tournament_years(self, tournament_data: dict) -> dict | None:
        soup = await self._get_html_async(tournament_data['t24_trn_archive_full_url'], need_soup=True)
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
        # print(tournament_data)
        return tournament_data

    async def load_tournaments(self):
        tasks = [self.__get_tournaments_links_by_trn_type_num(trn_type_num)
                 for trn_type_num in list(range(5724, 5743)) + [6393, 7897, 7898, 7899, 7900, 8430, 10883]]
        tournaments = await asyncio.gather(*tasks)
        tournaments = [trn for inner in tournaments if inner for trn in inner if trn]
        await self._dbo.insert_or_update_many('public', 't24_tournaments', tournaments,
                                              ['t24_trn_archive_full_url'], on_conflict_update=False)

    async def load_tournaments_years(self):
        tournaments_urls = await self._dbo.t24_get_tournaments_urls_to_load_years()
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
                    trn_years_data_to_db.append({'t24_trn_id': trn['id'],
                                                 't24_trn_year': year})
            await self._dbo.insert_or_update_many('public', 't24_tournaments_years', trn_years_data_to_db,
                                                  ['t24_trn_id', 't24_trn_year'], on_conflict_update=True)
            print(f'{len(trn_years_data_to_db)} records loaded to t24_tournaments_years')
            trns_with_years = {y['t24_trn_id'] for y in trn_years_data_to_db}
            trns_to_update = [{'id': trn_id, 't24_trn_years_loaded': True if trn_id in trns_with_years else False}
                              for trn_id in all_trns_in_batch]
            tasks = [self._dbo.update('public', 't24_tournaments', trn['id'],
                                      {'t24_trn_years_loaded': trn['t24_trn_years_loaded']})
                     for trn in trns_to_update]
            await asyncio.gather(*tasks)
            print(f'{len(tournaments_with_years)} t24_tournaments.t24_trn_years_loaded updated.')
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')

    async def init_database(self):
        await self._dbo.init_pool()
        await self._dbo.close_pg_connections()
        await self.load_tournaments()
        await self.load_tournaments_years()


def t24_init_database():
    t24 = T24InitDataBase()
    asyncio.run(t24.init_database())


if __name__ == '__main__':
    start_time = datetime.now()
    t24_init_database()

    print('Time length:', datetime.now() - start_time)
