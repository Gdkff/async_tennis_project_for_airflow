import json
import re

from dags_modules.t24_init import Tennis24, asyncio
from datetime import datetime, date, timedelta, timezone
from settings.config import tz


class T24DailyMatchesLoading(Tennis24):
    @staticmethod
    def __tournament_line_parsing(tournament_line: str):
        split_dict = {' - ': 'trn_category',
                      ': ': 'trn_type',
                      ' (': 'trn_name',
                      '), ': 'trn_country'}
        if 'MIXED DOUBLES' in tournament_line:
            split_dict = {' ': 'trn_category',
                          ': ': 'trn_type',
                          ' (': 'trn_name',
                          '), ': 'trn_country'}
        trn_data = {'qualification': False}
        if ' - Qualification' in tournament_line:
            trn_data['trn_qualification'] = True
            tournament_line = tournament_line.replace(' - Qualification', '')
        for line_part in tournament_line.split('¬'):
            if line_part[:3] == 'ZA÷':
                line = line_part.split('÷')[-1]
                for splitter, key in split_dict.items():
                    splitter_place = line.find(splitter)
                    trn_data[key] = line[:splitter_place]
                    line = line[splitter_place + len(splitter):]
                trn_data['surface'] = line
            elif line_part[:3] == 'ZE÷':
                trn_data['t24_first_draw_id'] = line_part.split('÷')[-1]
            elif line_part[:3] == 'ZC÷':
                trn_data['t24_main_draw_id'] = line_part.split('÷')[-1]
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

    def __match_line_parsing(self, match_line):
        split_dict = {'AA': 't24_match_id',
                      'AD': 'match_start',
                      'AO': 'match_finish',
                      'AB': 'match_status_short',
                      'AC': 'match_status',
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
                      't2_s5_score_tiebreak': None}
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

    async def load_daily_matches(self):
        await self._dbo.init_pool()
        matches = []
        for day_number in range(-1, 7):
            print('####### Day number:', str(day_number) + ', Date:', date.today() + timedelta(days=day_number))
            url = f'https://global.flashscore.ninja/107/x/feed/f_2_{day_number}_4_en_1'
            match_page = await super()._get_html_async(url, need_soup=False)
            current_tournament = {}
            for line in match_page.split('¬~'):
                if line[:3] == 'ZA÷':
                    current_tournament = self.__tournament_line_parsing(line)
                elif line[:2] == 'AA':
                    match_data = self.__match_line_parsing(line)
                    match_data.update({'trn_category': current_tournament.get('trn_category'),
                                       'trn_type': current_tournament.get('trn_type'),
                                       'trn_name': current_tournament.get('trn_name'),
                                       'trn_country': current_tournament.get('trn_country'),
                                       'surface': current_tournament.get('surface'),
                                       'trn_qualification': current_tournament.get('trn_qualification')})
                    matches.append(match_data)
        await self._dbo.insert_or_update_many('public', 't24_matches', matches, ['t24_match_id'])
        await self._dbo.close_pool()

    async def __get_initial_match_data_by_t24_match_id(self, match_players: dict) -> dict | None:
        match_soup = await self._get_html_async(match_players['match_url'])
        html_str = str(match_soup)
        start_place = html_str.find('"participantsData":{')
        end_place = html_str.find(',"eventParticipantEncodedId":')
        if start_place == -1 and end_place == -1:
            return {'t24_match_id': match_players['t24_match_id'],
                    'initial_players_data_loaded': False,
                    't1_pl1': None,
                    't1_pl2': None,
                    't2_pl1': None,
                    't2_pl2': None}
        teams = json.loads(html_str[start_place + 19:end_place])
        match_players_ids_out = {'t24_match_id': match_players['t24_match_id'],
                                 'initial_players_data_loaded': True}
        match_players_ids_out.update({f'{key}_id': None for key in ('t1_pl1', 't1_pl2', 't2_pl1', 't2_pl2')})
        for team_t24, team_db in {'home': 't1', 'away': 't2'}.items():
            for pl_num, pl in enumerate(teams[team_t24], 1):
                pl_name = match_players.get(f'{team_db}_pl{pl_num}_name')
                if pl_name == pl['name']:
                    if pl['id'] not in self._players:
                        pl_full_name, birthday = await self.__get_t24_pl_full_data(pl['id'])
                        self._new_players.append({'t24_pl_id': pl['id'],
                                                  'pl_url': f'https://www.tennis24.com{pl['detail_link']}',
                                                  'pl_name_short': pl['name'],
                                                  'pl_full_name': pl_full_name,
                                                  'country': pl['country'],
                                                  'birthday': birthday})
                        self._players.append(pl['id'])
                    match_players_ids_out.update({f'{team_db}_pl{pl_num}_id': pl['id']})
        return match_players_ids_out

    async def __load_all_players_from_db(self):
        players = await self._dbo.select('public', 't24_players', ['t24_pl_id'], where_conditions=None)
        self.__players = [player['t24_pl_id'] for player in players]

    async def __get_t24_pl_full_data(self, t24_pl_id: str) -> (str, datetime):
        player_soup = await self._get_html_async(f'https://www.tennis24.com/?r=4:{t24_pl_id}')
        birthday = None
        container__heading = player_soup.find('div', class_='container__heading')
        pl_full_name = container__heading.find('div', class_='heading__name').text
        scripts = container__heading.find_all('script')
        for script in scripts:
            script_text = script.text
            if 'getAge' in script_text:
                first_elm = script_text.find('getAge') + 7
                last_elm = first_elm + script_text[first_elm:].find(')')
                timestamp_str = script_text[first_elm:last_elm]
                if not timestamp_str.isdigit():
                    timestamp_str = re.sub(r"\D", "", timestamp_str)
                timestamp = int(timestamp_str)
                birthday = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
        return pl_full_name, birthday

    async def load_match_players_data(self):
        await self._dbo.init_pool()
        await self._dbo.close_pg_connections()
        await self.__load_all_players_from_db()
        matches = await self._dbo.t24_get_matches_without_initial_load()
        print(f'{len(matches)} not initial matches loaded from db')
        tasks = [self.__get_initial_match_data_by_t24_match_id(match) for match in matches]
        matches_update = await asyncio.gather(*tasks)
        if self._new_players:
            await self._dbo.insert_or_update_many('public', 't24_players', self._new_players, ['t24_pl_id'])
        await self._dbo.insert_or_update_many('public', 't24_matches', matches_update, ['t24_match_id'])
        await self._dbo.close_pool()

    async def __get_pbp_match_data(self, t24_match_id):
        url = f'https://global.flashscore.ninja/107/x/feed/df_mh_2_{t24_match_id}'
        bpb_match_string = await self._get_html_async(url, need_soup=False)
        pbp_match_data = self._bpb_parser.parse_t24_pbp_string(bpb_match_string, t24_match_id)
        return pbp_match_data

    async def __put_pbp_games_to_db(self, pbp_games: [dict]):
        all_dim_pbp_game_lines = await self._dbo.select('public', 'dim_game_pbp', ['server_points_line'])
        all_dim_pbp_game_lines = [x['server_points_line'] for x in all_dim_pbp_game_lines]
        new_dim_pbp_game_lines_to_db = []
        all_dim_pbp_tiebreak_lines = await self._dbo.select('public', 'dim_tiebreak_pbp', ['server_points_line'])
        all_dim_pbp_tiebreak_lines = [x['server_points_line'] for x in all_dim_pbp_tiebreak_lines]
        new_dim_pbp_tiebreak_lines_to_db = []
        pbp_games_to_db = []
        for g in pbp_games:
            if g['game'] != 13 and g['server_game_points_line'] not in all_dim_pbp_game_lines:
                new_dim_pbp_game_lines_to_db.append({
                    'server_points_line': g['server_game_points_line'],
                    'points_total': g['points_total'],
                    'server_points_won': g['server_points_won'],
                    'server_win': g['server_win'],
                    'receiver_points_won': g['receiver_points_won'],
                    'receiver_breakpoints': g['receiver_breakpoints'],
                    'receiver_breakpoints_converted': g['receiver_breakpoints_converted']
                })
                all_dim_pbp_game_lines.append(g['server_game_points_line'])
            if g['game'] == 13 and g['server_game_points_line'] not in all_dim_pbp_tiebreak_lines:
                new_dim_pbp_tiebreak_lines_to_db.append({
                    'server_points_line': g['server_game_points_line'],
                    'points_total': g['points_total'],
                    'first_server_win': g['first_server_win'],
                    'first_server_points_on_serve_line': g['first_server_points_on_serve_str'],
                    'second_server_points_on_serve_line': g['second_server_points_on_serve_str'],
                    'first_server_points_on_serve_won': g['first_server_points_on_serve_won'],
                    'first_server_points_on_receive_won': g['first_server_points_on_receive_won'],
                    'second_server_points_on_serve_won': g['second_server_points_on_serve_won'],
                    'second_server_points_on_receive_won': g['second_server_points_on_receive_won']
                })
                all_dim_pbp_tiebreak_lines.append(g['server_game_points_line'])
            pbp_games_to_db.append(
                {'t24_match_id': g['t24_match_id'],
                 'set': g['set'],
                 'game': g['game'],
                 'server': g['server'],
                 'server_game_points_line': g['server_game_points_line'] if g['game'] < 13 else None,
                 'server_tiebreak_points_line': g['server_game_points_line'] if g['game'] == 13 else None
                 })
        await self._dbo.insert_or_update_many('public', 'dim_game_pbp', new_dim_pbp_game_lines_to_db,
                                              ['server_points_line'])
        await self._dbo.insert_or_update_many('public', 'dim_tiebreak_pbp', new_dim_pbp_tiebreak_lines_to_db,
                                              ['server_points_line'])
        await self._dbo.insert_or_update_many('public', 't24_game_pbp', pbp_games_to_db,
                                              ['t24_match_id', 'set', 'game'])

    async def __get_match_statistic_by_match_id(self, t24_match_id: str) -> list:
        url = f'https://global.flashscore.ninja/107/x/feed/df_st_2_{t24_match_id}'
        data = await self._get_html_async(url, need_soup=False)
        db_stat_fields = ('aces', 'double_faults', 'service_points', 'first_serves_won', 'first_serves_success',
                          'second_serves_won', 'break_points_converted', 'break_points_created',
                          'average_first_serve_speed_km_h', 'average_second_serve_speed_km_h', 'winners',
                          'unforced_errors', 'net_points_won', 'net_approaches')
        stats_keys_dict = {'Aces': ('aces',),
                           'Double Faults': ('double_faults',),
                           'Service Points Won': (None, None, 'service_points'),
                           '1st Serve Points Won': (None, 'first_serves_won', 'first_serves_success'),
                           '2nd Serve Points Won': (None, 'second_serves_won', None),
                           'Break Points Converted': (None, 'break_points_converted', 'break_points_created'),
                           'Average 1st Serve Speed': ('average_first_serve_speed_km_h',),
                           'Average 2nd Serve Speed': ('average_second_serve_speed_km_h',),
                           'Winners': ('winners',),
                           'Unforced Errors': ('unforced_errors',),
                           'Net Points Won': (None, 'net_points_won', 'net_approaches')
                           }
        delimiters_dict = {'% (': ('% (', '/', ')'),
                           ' km/h': ' km/h',
                           '': None}
        data = data.split('¬~')
        match_statistic = []
        section, period, key_out, t1_val, t2_val, statistic = None, None, None, None, None, None
        period_num = 0
        for part in data:
            parts_of_part = part.split('¬')
            if part[:2] in ('SE', 'A1') and period_num > 0:
                t1_sum_to_check = 0
                t2_sum_to_check = 0
                for stat_field in db_stat_fields:
                    t1_sum_to_check += statistic['t1'][stat_field] if statistic['t1'].get(stat_field) else 0
                    t2_sum_to_check += statistic['t2'][stat_field] if statistic['t2'].get(stat_field) else 0
                if t1_sum_to_check > 0:
                    match_statistic.append(statistic['t1'].copy())
                if t2_sum_to_check > 0:
                    match_statistic.append(statistic['t2'].copy())
            if part[:2] == 'SE':
                period_num += 1
                period = parts_of_part[0].split('÷')[1]
                period = 0 if period == 'Match' else int(period.replace('Set ', ''))
                statistic = {'t1': {'t24_match_id': t24_match_id,
                                    'team_num': 1,
                                    'set': period},
                             't2': {'t24_match_id': t24_match_id,
                                    'team_num': 2,
                                    'set': period}}
                empty_fields_dict = {x: None for x in db_stat_fields}
                statistic['t1'].update(empty_fields_dict.copy())
                statistic['t2'].update(empty_fields_dict.copy())
            if part[:2] == 'SG':
                for part_of_part in parts_of_part:
                    key, value = part_of_part.split('÷')
                    if key == 'SG':
                        key_out = value
                    if key == 'SH':
                        t1_val = value
                    if key == 'SI':
                        t2_val = value
                if key_out in stats_keys_dict:
                    db_keys = stats_keys_dict[key_out]
                    for team_num, team_value in enumerate((t1_val, t2_val), 1):
                        team_line_vals = []
                        for check_val in delimiters_dict:
                            if check_val in team_value:
                                delimiters = delimiters_dict[check_val]
                                if not delimiters:
                                    team_line_vals = [int(team_value)]
                                    break
                                for delimiter in delimiters:
                                    team_value = team_value.replace(delimiter, '#')
                                t = team_value.strip('#')
                                t = t.split('#')
                                team_line_vals = [int(x) for x in t]
                                break
                        result = {k: v for k, v in zip(db_keys, team_line_vals) if k is not None}
                        statistic[f't{team_num}'].update(result)
        return match_statistic

    async def load_final_match_data(self):
        await self._dbo.init_pool()
        await self._dbo.close_pg_connections()
        matches_not_loaded_pbp = [match['t24_match_id']
                                  for match in await self._dbo.t24_get_ended_matches_non_loaded_pbp()]
        print(f'{len(matches_not_loaded_pbp)} ended matches without PbP loaded')
        tasks = [self.__get_pbp_match_data(t24_match_id) for t24_match_id in matches_not_loaded_pbp]
        pbp_matches = await asyncio.gather(*tasks)
        pbp_games = [pbp_game for match in pbp_matches if match for pbp_game in match if pbp_game]
        print('PbP data downloaded')
        await self.__put_pbp_games_to_db(pbp_games)
        matches_loaded_pbp = {x['t24_match_id'] for x in pbp_games}
        update_matches_pbp = [{'t24_match_id': t24_match_id,
                               'final_pbp_data_loaded': True if t24_match_id in matches_loaded_pbp else False}
                              for t24_match_id in matches_not_loaded_pbp]
        await self._dbo.insert_or_update_many('public', 't24_matches', update_matches_pbp,
                                              ['t24_match_id'])
        print('PbP data uploaded to db')
        matches_not_loaded_statistics = [match['t24_match_id']
                                         for match in await self._dbo.t24_get_ended_matches_non_loaded_statistics()]
        print(f'{len(matches_not_loaded_statistics)} ended matches without statistics loaded')
        tasks = [self.__get_match_statistic_by_match_id(t24_match_id) for t24_match_id in matches_not_loaded_statistics]
        sets_statistic = await asyncio.gather(*tasks)
        sets_statistic = [set_stat for inner in sets_statistic if inner for set_stat in inner if set_stat]
        print('Statistics data downloaded')
        await self._dbo.insert_or_update_many('public', 't24_set_statistics', sets_statistic,
                                              ['t24_match_id', 'team_num', 'set'])
        matches_loaded_statistics = {x['t24_match_id'] for x in sets_statistic}
        update_matches_stat = [{'t24_match_id': t24_match_id,
                                'final_statistics_data_loaded': True if t24_match_id in matches_loaded_statistics else False}
                               for t24_match_id in matches_not_loaded_statistics]
        await self._dbo.insert_or_update_many('public', 't24_matches', update_matches_stat,
                                              ['t24_match_id'])
        print('Statistics data uploaded to db')
        await self._dbo.close_pool()


def t24_load_daily_matches():
    t24 = T24DailyMatchesLoading()
    asyncio.run(t24.load_daily_matches())


def t24_load_initial_match_data():
    t24 = T24DailyMatchesLoading()
    asyncio.run(t24.load_match_players_data())


def t24_load_final_match_data():
    t24 = T24DailyMatchesLoading()
    asyncio.run(t24.load_final_match_data())


if __name__ == '__main__':
    start_time = datetime.now()
    # t24_load_daily_matches()
    # t24_load_initial_match_data()
    t24_load_final_match_data()

    print('Time length:', datetime.now() - start_time)
