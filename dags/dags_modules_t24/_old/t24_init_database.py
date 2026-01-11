from dags_modules_t24.t24_init import Tennis24, asyncio
from datetime import datetime
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

    @staticmethod
    def __generate_score_string_from_match_data(match_data: dict) -> str:
        score_string = ''
        for set_num in range(1, 6):
            for team_num in range(1, 3):
                score = match_data.get(f't{team_num}_s{set_num}_score')
                score_tiebreak = match_data.get(f't{team_num}_s{set_num}_score_tiebreak')
                score_string += score if score else ''
                score_string += ('(' + score_tiebreak + ')') if score_tiebreak else ''
                score_string += '-' if score and team_num == 1 else ''
                score_string += ', ' if score and team_num == 2 else ''
        return score_string[:-2]

    def __parse_result_match_line(self, match_line_dict: dict) -> dict:
        split_dict = {'AA': 't24_match_id',
                      'AD': 'match_start',
                      'AO': 'match_finish',
                      'AB': 'match_status_short',
                      'AC': 'match_status',
                      'ER': 'trn_stage',
                      'FH': 't1_pl1_name',
                      'FJ': 't1_pl2_name',
                      'FU': 't1_pl1_country',
                      'FW': 't1_pl2_country',
                      'FK': 't2_pl1_name',
                      'FL': 't2_pl2_name',
                      'FV': 't2_pl1_country',
                      'FX': 't2_pl2_country',
                      'AS': 'team_winner',
                      'AG': 't1_sets_won',
                      'AH': 't2_sets_won',
                      'BA': 't1_s1_score',
                      'DA': 't1_s1_score_tiebreak',
                      'BB': 't2_s1_score',
                      'DB': 't2_s1_score_tiebreak',
                      'BC': 't1_s2_score',
                      'DC': 't1_s2_score_tiebreak',
                      'BD': 't2_s2_score',
                      'DD': 't2_s2_score_tiebreak',
                      'BE': 't1_s3_score',
                      'DE': 't1_s3_score_tiebreak',
                      'BF': 't2_s3_score',
                      'DF': 't2_s3_score_tiebreak',
                      'BG': 't1_s4_score',
                      'DG': 't1_s4_score_tiebreak',
                      'BH': 't2_s4_score',
                      'DH': 't2_s4_score_tiebreak',
                      'BI': 't1_s5_score',
                      'DI': 't1_s5_score_tiebreak',
                      'BJ': 't2_s5_score',
                      'DJ': 't2_s5_score_tiebreak'}
        match_statuses = {'1': 'Not started',
                          '3': 'Finished',
                          '5': 'Cancelled',
                          '8': 'Finished (retried)',
                          '9': 'Walkover',
                          '17': 'Live Set 1',
                          '18': 'Live Set 2',
                          '19': 'Live Set 3',
                          '20': 'Live Set 4',
                          '21': 'Live Set 5',
                          '36': 'Interrupted',
                          '46': 'Interrupted',
                          '47': 'Live Set 1 Tiebreak',
                          '48': 'Live Set 2 Tiebreak',
                          '49': 'Live Set 3 Tiebreak',
                          '50': 'Live Set 4 Tiebreak',
                          '51': 'Live Set 5 Tiebreak'}
        match_statuses_short = {'1': 'Not started',
                                '2': 'Playing',
                                '3': 'Ended'}
        int_fields = ['team_winner', 't1_sets_won', 't2_sets_won', 't1_s1_score', 't1_s1_score_tiebreak', 't2_s1_score',
                      't2_s1_score_tiebreak', 't1_s2_score', 't1_s2_score_tiebreak', 't2_s2_score',
                      't2_s2_score_tiebreak', 't1_s3_score', 't1_s3_score_tiebreak', 't2_s3_score',
                      't2_s3_score_tiebreak',
                      't1_s4_score', 't1_s4_score_tiebreak', 't2_s4_score', 't2_s4_score_tiebreak', 't1_s5_score',
                      't1_s5_score_tiebreak', 't2_s5_score', 't2_s5_score_tiebreak']
        match_data = {'t24_match_id': None,
                      'trn_stage': None,
                      'match_start': None,
                      'match_finish': None,
                      'match_status_short': None,
                      'match_status': None,
                      't1_pl1_name': None,
                      't1_pl2_name': None,
                      't1_pl1_country': None,
                      't1_pl2_country': None,
                      't2_pl1_name': None,
                      't2_pl2_name': None,
                      't2_pl1_country': None,
                      't2_pl2_country': None,
                      'team_winner': None,
                      'match_score': None,
                      't1_sets_won': None,
                      't2_sets_won': None,
                      't1_s1_score': None,
                      't1_s1_score_tiebreak': None,
                      't2_s1_score': None,
                      't2_s1_score_tiebreak': None,
                      't1_s2_score': None,
                      't1_s2_score_tiebreak': None,
                      't2_s2_score': None,
                      't2_s2_score_tiebreak': None,
                      't1_s3_score': None,
                      't1_s3_score_tiebreak': None,
                      't2_s3_score': None,
                      't2_s3_score_tiebreak': None,
                      't1_s4_score': None,
                      't1_s4_score_tiebreak': None,
                      't2_s4_score': None,
                      't2_s4_score_tiebreak': None,
                      't1_s5_score': None,
                      't1_s5_score_tiebreak': None,
                      't2_s5_score': None,
                      't2_s5_score_tiebreak': None,
                      'initial_players_data_loaded': None}
        for key, value in match_line_dict.items():
            if key in split_dict:
                if split_dict[key] in ('match_start', 'match_finish'):
                    match_data[split_dict[key]] = datetime.fromtimestamp(int(value), tz=tz)
                else:
                    match_data[split_dict[key]] = value
        match_data.update({
            'match_status_short_code': int(match_data['match_status_short']),
            'match_status_short': match_statuses_short[match_data['match_status_short']],
            'match_status_code': int(match_data['match_status']),
            'match_status': match_statuses.get(match_data['match_status']),
            'match_url': f'https://www.tennis24.com/match/{match_data['t24_match_id']}/#/match-summary'})
        score_string = self.__generate_score_string_from_match_data(match_data)
        if score_string:
            match_data['match_score'] = score_string
        for field in int_fields:
            if match_data.get(field) is not None:
                match_data[field] = int(match_data[field])
        return match_data

    def __operate_tournament_lines(self, tournament_data: list, trn_year_id: int):
        tournament_matches_out = []
        qualification = None
        for line in tournament_data:
            if line.get('ZA') is not None:
                if 'Qualification' in line['ZA']:
                    qualification = True
                else:
                    qualification = None
            if line.get('AA') is not None:
                match = self.__parse_result_match_line(line)
                match['trn_qualification'] = qualification
                match['trn_year_id'] = trn_year_id
                tournament_matches_out.append(match)
        return tournament_matches_out

    async def __get_tournament_results(self, trn_in: dict) -> [dict]:
        url = (f'https://www.tennis24.com/'
               f'{trn_in["t24_trn_type"]}/{trn_in["t24_trn_name"]}-{trn_in["t24_trn_year"]}/results/')
        # url = 'https://www.tennis24.com/atp-singles/australian-open-2024/results/'
        # url = 'https://www.tennis24.com/atp-doubles/halle-2024/results/'
        tournament_soup = await self._get_html_async(url)
        tournament_html = str(tournament_soup)
        country_id_start = tournament_html.find('country_id = ') + 13
        country_id_end = tournament_html.find(';', country_id_start)
        country_id = tournament_html[country_id_start:country_id_end]
        tournament_id_start = tournament_html.find('"', country_id_end) + 1
        tournament_id_end = tournament_html.find('"', tournament_id_start)
        tournament_id = tournament_html[tournament_id_start:tournament_id_end]
        season_id_start = tournament_html.find('seasonId: ') + 10
        season_id_end = tournament_html.find(',', season_id_start)
        season_id = tournament_html[season_id_start:season_id_end]
        tournament_lines = []
        match_ids = set()
        tournament_draws = set()
        page_counter = 0
        while True:
            url = (f'https://global.flashscore.ninja/107/x/feed/'
                   f'tr_2_{country_id}_{tournament_id}_{season_id}_{page_counter}_4_en_2')
            trn_results_raw = await self._get_html_async(url, need_soup=False)
            if not trn_results_raw:
                break
            tournament_split = trn_results_raw.split('¬~')
            for line in tournament_split:
                split_line = line.split('¬')
                line_dict = dict(teg.split('÷', 1) for teg in split_line if '÷' in teg)
                for key, var in {'AA': match_ids, 'ZA': tournament_draws}.items():
                    if line[:2] == key:
                        # [print(f'{key}: {val}') for key, val in line_dict.items()]
                        if line_dict[key] not in var:
                            var.add(line_dict[key])
                            tournament_lines.append(line_dict)
                        else:
                            continue
            page_counter += 1
        matches = self.__operate_tournament_lines(tournament_lines, trn_in["id"])
        return matches

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

    async def load_tournaments_results(self):
        tournaments = await self._dbo.t24_get_tournaments_years_to_load_results()
        batch_size = self._concurrency
        batches = [tournaments[i:i + batch_size] for i in range(0, len(tournaments), batch_size)]
        batches_count = len(batches)
        for batch in batches[:1]:
            in_time = datetime.now()
            trn_years_ids_all = {x['id'] for x in batch[:10]}
            tasks = [self.__get_tournament_results(trn) for trn in batch]
            tournaments_results = await asyncio.gather(*tasks)
            tournaments_results = [x for inner in tournaments_results if inner for x in inner if x]
            await self._dbo.insert_or_update_many('public', 't24_matches', tournaments_results, ['t24_match_id'])
            print(f'{len(tournaments_results)} records loaded to t24_matches')
            tasks = [self._get_initial_match_data_by_t24_match_id(
                {'t24_match_id': match['t24_match_id'],
                 'match_url': match['match_url'],
                 't1_pl1_name': match['t1_pl1_name'],
                 't1_pl2_name': match['t1_pl2_name'],
                 't2_pl1_name': match['t2_pl1_name'],
                 't2_pl2_name': match['t2_pl2_name']}) for match in tournaments_results]
            pl_ids = await asyncio.gather(*tasks)
            await self._dbo.insert_or_update_many('public', 't24_matches', pl_ids, ['t24_match_id'])
            trn_year_ids_exists = {x['trn_year_id'] for x in tournaments_results}
            # trn_years_update = [{'id': x, 't24_results_loaded': True if x in trn_year_ids_exists else False}
            #                     for x in trn_years_ids_all]
            # tasks = [self._dbo.update('public',
            #                           't24_tournaments_years',
            #                           trn['id'],
            #                           {'t24_results_loaded': trn['t24_results_loaded']})
            #          for trn in trn_years_update]
            # await asyncio.gather(*tasks)
            print(f'{len(trn_years_ids_all)} t24_tournaments_years.t24_results_loaded updated.')
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')

    async def init_database(self):
        await self._dbo.init_pool()
        await self._dbo.close_pg_connections()
        await self._load_all_players_from_db()
        # await self.load_tournaments()
        # await self.load_tournaments_years()
        await self.load_tournaments_results()


def t24_init_database():
    t24 = T24InitDataBase()
    asyncio.run(t24.init_database())


if __name__ == '__main__':
    start_time = datetime.now()
    t24_init_database()

    print('Time length:', datetime.now() - start_time)
