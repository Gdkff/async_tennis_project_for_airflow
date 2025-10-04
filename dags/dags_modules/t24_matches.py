from dags_modules.t24_init import Tennis24, asyncio
from datetime import datetime, date, timedelta
from settings.config import tz
import re
from dags_modules.t24_tournaments import T24Tournaments
from dags_modules.t24_players import T24Players
import json


class T24Matches(Tennis24):
    def __init__(self):
        super().__init__()
        self.__all_trn_years = {}
        self.T24Tournaments = None
        self.all_tournaments_years = dict()
        self.T24Players = None
        self.all_players = set()
        self.__new_players = set()

    async def __load_all_trn_years(self):
        trn_years = await self._dbo.select('public', 't24_tournaments_years', ['id', 'first_draw_id'])
        self.__all_trn_years = {t['first_draw_id']: t['id'] for t in trn_years if t['first_draw_id']}

    async def __tournament_line_parsing(self, tournament_line: str):
        trn_data = {'is_qualification': False}
        if ' - Qualification' in tournament_line:
            trn_data['is_qualification'] = True
            tournament_line = tournament_line.replace(' - Qualification', '')
        for line_part in tournament_line.split('¬'):
            if line_part[:3] == 'ZE÷':
                first_draw_id = line_part.split('÷')[-1]
                trn_data['trn_year_id'] = self.__all_trn_years.get(first_draw_id)
            if line_part[:3] == 'ZL÷':
                url_part = line_part.split('÷')[-1]
                url_part_split = url_part.split('/')
                trn_data.update({'trn_archive_full_url': f'https://www.tennis24.com{url_part}archive/',
                                 'trn_type': url_part_split[1],
                                 'trn_name': url_part_split[2]})
        if trn_data.get('trn_year_id') is None:
            print('!!! trn_year_id is null')
            print(trn_data)
            await self.T24Tournaments.load_tournaments(tournament_to_load=trn_data, on_conflict_update=True)
            await self.T24Tournaments.load_tournaments_years()
            await self.T24Tournaments.load_tournaments_draws_id()
        return trn_data

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

    @staticmethod
    def __add_indexes(s: str) -> str:
        counters = {}

        def repl(match):
            key = match.group(1)
            counters[key] = counters.get(key, 0) + 1
            return f"¬{key}{counters[key]}÷"

        return re.sub(r"¬([A-Z]+)÷", repl, s)

    def __match_line_parsing(self, match_line):
        split_dict = {'AA': 't24_match_id',
                      'AD1': 'match_start',
                      'AO1': 'match_finish',
                      'AB1': 'match_status_short',
                      'AC1': 'match_status',
                      'PX1': 't1_pl1_id',
                      'PX2': 't1_pl2_id',
                      'PY1': 't2_pl1_id',
                      'PY2': 't2_pl2_id',
                      'AS1': 'team_winner',
                      'AG1': 't1_sets_won',
                      'AH1': 't2_sets_won',
                      'BA1': 't1_s1_score',
                      'DA1': 't1_s1_score_tiebreak',
                      'BB1': 't2_s1_score',
                      'DB1': 't2_s1_score_tiebreak',
                      'BC1': 't1_s2_score',
                      'DC1': 't1_s2_score_tiebreak',
                      'BD1': 't2_s2_score',
                      'DD1': 't2_s2_score_tiebreak',
                      'BE1': 't1_s3_score',
                      'DE1': 't1_s3_score_tiebreak',
                      'BF1': 't2_s3_score',
                      'DF1': 't2_s3_score_tiebreak',
                      'BG1': 't1_s4_score',
                      'DG1': 't1_s4_score_tiebreak',
                      'BH1': 't2_s4_score',
                      'DH1': 't2_s4_score_tiebreak',
                      'BI1': 't1_s5_score',
                      'DI1': 't1_s5_score_tiebreak',
                      'BJ1': 't2_s5_score',
                      'DJ1': 't2_s5_score_tiebreak'}
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
                      'match_start': None,
                      'match_finish': None,
                      'match_status_short': None,
                      'match_status': None,
                      't1_pl1_id': None,
                      't1_pl2_id': None,
                      't2_pl1_id': None,
                      't2_pl2_id': None,
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
                      't2_s5_score_tiebreak': None}
        match_line = self.__add_indexes(match_line)
        for line_part in match_line.split('¬'):
            key, value = line_part.split('÷')
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

    async def __get_and_put_all_new_players_from_matches(self, matches):
        for match in matches:
            if match['t1_pl1_id'] and match['t1_pl1_id'] not in self.all_players:
                self.__new_players.add(match['t1_pl1_id'])
            if match['t1_pl2_id'] and match['t1_pl2_id'] not in self.all_players:
                self.__new_players.add(match['t1_pl2_id'])
            if match['t2_pl1_id'] and match['t2_pl1_id'] not in self.all_players:
                self.__new_players.add(match['t2_pl1_id'])
            if match['t2_pl2_id'] and match['t2_pl2_id'] not in self.all_players:
                self.__new_players.add(match['t2_pl2_id'])
        new_players = [{'t24_pl_id': np} for np in self.__new_players]
        await self._dbo.insert_or_update_many('public', 't24_players', new_players, ['t24_pl_id'], on_conflict_update=False)
        self.all_players.update(self.__new_players)
        self.__new_players = set()

    async def load_daily_matches(self):
        await self._dbo.init_pool()
        await self._dbo.close_pg_connections()
        await self.__load_all_trn_years()
        self.T24Tournaments = T24Tournaments()
        await self.T24Tournaments.init_async()
        self.T24Players = T24Players()
        await self.T24Players.init_async()
        matches = []
        for day_number in range(-1, 7):
            print('####### Day number:', str(day_number) + ', Date:', date.today() + timedelta(days=day_number))
            url = f'https://global.flashscore.ninja/107/x/feed/f_2_{day_number}_4_en_1'
            match_page = await super()._get_html_async(url, need_soup=False)
            current_tournament = {}
            for line in match_page.split('¬~'):
                if line[:3] == 'ZA÷':
                    current_tournament = await self.__tournament_line_parsing(line)
                elif line[:2] == 'AA':
                    match_data = self.__match_line_parsing(line)
                    match_data.update({'trn_year_id': current_tournament.get('trn_year_id'),
                                       'is_qualification': current_tournament.get('is_qualification')})
                    matches.append(match_data)
        await self.__get_and_put_all_new_players_from_matches(matches)
        await self.T24Players.load_players_data()
        await self._dbo.insert_or_update_many('public', 't24_matches', matches, ['t24_match_id'])
        await self._dbo.close_pool()


def t24_load_daily_matches():
    t24 = T24Matches()
    asyncio.run(t24.load_daily_matches())


if __name__ == '__main__':
    start_time = datetime.now()
    t24_load_daily_matches()
    # t24_load_initial_match_data()
    # t24_load_final_match_data()

    print('Time length:', datetime.now() - start_time)
