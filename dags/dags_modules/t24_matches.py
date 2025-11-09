from dags_modules.dbo import DBOperator
from dags_modules.t24_init import Tennis24
from datetime import datetime, date, timedelta
from settings.config import tz
import re


class T24Matches(Tennis24):
    def __init__(self, dbo_in: DBOperator):
        super().__init__()
        self.__dbo = dbo_in
        self.__daily_match_pages = list()

    async def get_daily_match_pages(self):
        daily_match_pages = []
        for day_number in range(-1, 8):
            print('####### Day number:', str(day_number) + ', Date:', date.today() + timedelta(days=day_number))
            url = f'https://global.flashscore.ninja/107/x/feed/f_2_{day_number}_4_en_1'
            daily_match_pages += [await super()._get_html_async(url, need_soup=False)]
        self.__daily_match_pages = daily_match_pages

    async def get_new_tournaments_and_years(self, all_tournament_draw_ids: dict) -> list[dict]:
        new_tournaments = list()
        url_set = set()
        for match_page in self.__daily_match_pages:
            for line in match_page.split('¬~'):
                if line[:3] == 'ZA÷':
                    tournament = await self.__tournament_line_parsing(line, all_tournament_draw_ids)
                    if tournament['trn_year_id'] is None:
                        if tournament['trn_archive_full_url'] not in url_set:
                            url_set.add(tournament['trn_archive_full_url'])
                            new_tournaments.append(tournament)
        return new_tournaments

    async def get_matches_from_pages(self,
                                     all_tournament_draw_ids: dict,
                                     match_pages_in: list[tuple] = None) -> tuple[list[dict], list[dict]]:
        correct_matches = list()
        defective_matches = list()
        match_pages = match_pages_in if match_pages_in else [(page, None) for page in self.__daily_match_pages]
        for match_page, trn_year_id in match_pages:
            current_tournament = {}
            for line in match_page.split('¬~'):
                if line[:3] == 'ZA÷':
                    current_tournament = await self.__tournament_line_parsing(line, all_tournament_draw_ids)
                elif line[:2] == 'AA':
                    match_data = self.__match_line_parsing(line)
                    match_data.update({'trn_year_id': trn_year_id if trn_year_id
                                       else current_tournament.get('trn_year_id'),
                                       'is_qualification': current_tournament.get('is_qualification')})
                    if match_data['trn_year_id'] is not None:
                        correct_matches.append(match_data)
                    else:
                        defective_matches.append(match_data)
        return correct_matches, defective_matches

    @staticmethod
    async def __tournament_line_parsing(tournament_line: str, all_tournament_draw_ids: dict):
        trn_data = {'is_qualification': False}
        if ' - Qualification' in tournament_line:
            trn_data['is_qualification'] = True
            tournament_line = tournament_line.replace(' - Qualification', '')
        for line_part in tournament_line.split('¬'):
            if line_part[:3] == 'ZE÷':
                first_draw_id = line_part.split('÷')[-1]
                trn_data['trn_year_id'] = all_tournament_draw_ids.get(first_draw_id)
            if line_part[:3] == 'ZL÷':
                url_part = line_part.split('÷')[-1]
                url_part_split = url_part.split('/')
                trn_data.update({'trn_archive_full_url': f'https://www.tennis24.com{url_part}archive/',
                                 'trn_type': url_part_split[1],
                                 'trn_name': url_part_split[2]})
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
                      'AD': 'match_start',
                      'AO': 'match_finish',
                      'AB': 'match_status_short',
                      'AC': 'match_status',
                      'PX': 't1_pl1_id',
                      'PY': 't2_pl1_id',
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
        # match_line = self.__add_indexes(match_line)
        for line_part in match_line.split('¬'):
            key, value = line_part.split('÷')
            if key in split_dict:
                if split_dict[key] in ('match_start', 'match_finish'):
                    match_data[split_dict[key]] = datetime.fromtimestamp(int(value), tz=tz)
                else:
                    match_data[split_dict[key]] = value
        if '/' in match_data['t1_pl1_id']:
            match_data['t1_pl1_id'], match_data['t1_pl2_id'] = match_data['t1_pl1_id'].split('/')
        if '/' in match_data['t2_pl1_id']:
            match_data['t2_pl1_id'], match_data['t2_pl2_id'] = match_data['t2_pl1_id'].split('/')
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
                match_data[field] = int(match_data[field].replace('/', ''))
        return match_data

    async def pbp_get_match_data(self, t24_match_id):
        url = f'https://global.flashscore.ninja/107/x/feed/df_mh_2_{t24_match_id}'
        bpb_match_string = await self._get_html_async(url, need_soup=False)
        if not bpb_match_string:
            return None
        pbp_match_data = self.pbp_parse_t24_string(bpb_match_string, t24_match_id)
        return pbp_match_data

    @staticmethod
    def __pbp_parse_game_points(points: list, server: int, winner: int) -> list:
        game_data = []
        server -= 1
        winner -= 1
        receiver = 0 if server == 1 else 1
        for point_num, point in enumerate(points):
            if point_num == 0:
                game_data.append(1 if point[server] > point[receiver] else 0)
            elif point[receiver] == 50:
                game_data.append(0)
            elif point[server] == 40 and point[receiver] == 40 and points[point_num - 1][receiver] == 50:
                game_data.append(1)
            else:
                game_data.append(1 if point[server] > points[point_num - 1][server] else 0)
        game_data.append(1 if winner == server else 0)
        return game_data

    @staticmethod
    def __pbp_parse_tiebreak(points: list, t24_match_id: str, set_num: int):
        # print(points)
        first_server_team = points[0][2] - 1
        tiebreak_points_list = []
        first_server_points = []
        second_server_points = []
        for point_num, point in enumerate(points):
            server = point[2] - 1
            receiver = 1 if server == 0 else 0
            if point_num == 0:
                server_point = 1 if point[server] > point[receiver] else 0
            else:
                server_point = 1 if point[server] > points[point_num - 1][server] else 0
            if first_server_team == server:
                first_server_points.append(server_point)
            else:
                second_server_points.append(server_point)
            tiebreak_points_list.append(server_point)
        # print(tiebreak_points_list)
        first_server_points_on_serve_won = sum(first_server_points)
        first_server_points_on_receive_won = sum([1 if x == 0 else 0 for x in second_server_points])
        second_server_points_on_serve_won = sum(second_server_points)
        second_server_points_on_receive_won = sum([1 if x == 0 else 0 for x in first_server_points])
        points_total = len(tiebreak_points_list)
        tiebreak_pbp_data = {
            't24_match_id': t24_match_id,
            'set': set_num,
            'game': 13,
            'server': first_server_team + 1,
            'server_game_points_line': ''.join([str(x) for x in tiebreak_points_list]),
            'points_total': points_total,
            'first_server_win': True if first_server_points[-1] == 1 else False,
            'first_server_points_on_serve_str': ''.join([str(x) for x in first_server_points]),
            'second_server_points_on_serve_str': ''.join([str(x) for x in second_server_points]),
            'first_server_points_on_serve_won': first_server_points_on_serve_won,
            'first_server_points_on_receive_won': first_server_points_on_receive_won,
            'second_server_points_on_serve_won': second_server_points_on_serve_won,
            'second_server_points_on_receive_won': second_server_points_on_receive_won
        }
        return tiebreak_pbp_data

    def __pbp_parse_game_line(self, t24_match_id: str, line_dict: dict,
                              set_num: int, game_num: int, t1_bp: list, t2_bp: list):
        # print('serve:', line_dict['HG'], 'winner:', line_dict['HK'], 'T1:', line_dict['HC'], 'T2', line_dict['HE'])
        # print(t24_match_id, line_dict)
        server = int(line_dict['HG'])
        winner = int(line_dict['HK'])
        game_pbp = line_dict['HL']
        point_num = 1
        points = []
        receiver_breakpoints = 0
        for point in game_pbp.split(', '):
            game_points = point.split(':')
            if line_dict['HG'] == '1':
                if '|B1|' in point:
                    t2_bp.append((set_num, game_num, point_num))
                    receiver_breakpoints += 1
            elif line_dict['HG'] == '2':
                if '|B1|' in point:
                    t1_bp.append((set_num, game_num, point_num))
                    receiver_breakpoints += 1
            points.append((int(game_points[0].replace('AD', '50').replace('A', '50')),
                           int(game_points[1].replace(' |B1|', '').replace(' |B2|', '').replace(' |B3|', '').replace(
                               'AD', '50').replace('A', '50').strip())))
            # print(f'*{point}*')
            point_num += 1
        server_points_list = self.__pbp_parse_game_points(points, server, winner)
        server_points_str = ''.join([str(x) for x in server_points_list])
        server_points_won = sum(server_points_list)
        points_total = len(server_points_list)
        receiver_points_won = points_total - server_points_won
        server_win = server == winner
        receiver_breakpoints_converted = 1 if not server_win else 0
        game_pbp_data = {
            't24_match_id': t24_match_id,
            'set': set_num,
            'game': game_num,
            'server': server,
            'server_game_points_line': server_points_str,
            'points_total': points_total,
            'server_points_won': server_points_won,
            'server_win': server_win,
            'receiver_points_won': receiver_points_won,
            'receiver_breakpoints': receiver_breakpoints,
            'receiver_breakpoints_converted': receiver_breakpoints_converted
        }
        return game_pbp_data

    def pbp_parse_t24_string(self, t24_pbp: str, t24_match_id: str):
        pbp_split = t24_pbp.split('¬~')
        game_num = 1
        t1_bp = []
        t2_bp = []
        tiebreak_points = []
        set_num = 1
        match_pbp_data_out = []
        for line in pbp_split:
            # print(line)
            line_split = line.split('¬')
            line_dict = {}
            # set_or_tb = 'set'
            for line_part in line_split:
                if '÷' in line_part:
                    key, value = line_part.split('÷')
                    line_dict[key] = value
            if line[:2] == 'HA':
                if tiebreak_points:
                    match_pbp_data_out.append(self.__pbp_parse_tiebreak(tiebreak_points, t24_match_id, set_num))
                    tiebreak_points = []
                # set_or_tb = 'set'
                try:
                    set_num = int(line_dict['HB'].split(' - ')[-1].split()[-1])
                except IndexError:
                    continue
                # print('HA', set_or_tb)
                game_num = 1
            elif line[:2] == 'A1':
                if tiebreak_points:
                    match_pbp_data_out.append(self.__pbp_parse_tiebreak(tiebreak_points, t24_match_id, set_num))
                    tiebreak_points = []
            elif line[:2] == 'HB':
                # set_or_tb = 'tiebreak'
                set_num = int(line_dict['HB'].split(' - ')[-1].split()[-1])
                # print('HB', set_or_tb)
                tiebreak_points = []
            elif line[:2] == 'HC':
                if not line_dict.get('HL') or not line_dict.get('HG') or not line_dict.get('HK'):
                    if line_dict.get('HD'):
                        continue
                    # print('HC', line_dict)
                    tiebreak_points.append((int(line_dict['HC']), int(line_dict['HE']), int(line_dict['HG'])))
                    continue
                # print(f'Game {game_num}')
                game_data = self.__pbp_parse_game_line(t24_match_id, line_dict, set_num, game_num, t1_bp, t2_bp)
                game_num += 1
                match_pbp_data_out.append(game_data)
        return match_pbp_data_out

    async def pbp_put_games_to_db(self, pbp_games: list[dict]):
        all_dim_pbp_game_lines = await self.__dbo.select('public', 'dim_game_pbp', ['server_points_line'])
        all_dim_pbp_game_lines = {x['server_points_line'] for x in all_dim_pbp_game_lines}
        new_dim_pbp_game_lines_to_db = []
        all_dim_pbp_tiebreak_lines = await self.__dbo.select('public', 'dim_tiebreak_pbp', ['server_points_line'])
        all_dim_pbp_tiebreak_lines = {x['server_points_line'] for x in all_dim_pbp_tiebreak_lines}
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
                all_dim_pbp_game_lines.add(g['server_game_points_line'])
            if g['game'] == 13 and g['server_game_points_line'] not in all_dim_pbp_tiebreak_lines:
                new_dim_pbp_tiebreak_lines_to_db.append({
                    'server_points_line': g.get('server_game_points_line'),
                    'points_total': g.get('points_total'),
                    'first_server_win': g.get('first_server_win'),
                    'first_server_points_on_serve_line': g.get('first_server_points_on_serve_str'),
                    'second_server_points_on_serve_line': g.get('second_server_points_on_serve_str'),
                    'first_server_points_on_serve_won': g.get('first_server_points_on_serve_won'),
                    'first_server_points_on_receive_won': g.get('first_server_points_on_receive_won'),
                    'second_server_points_on_serve_won': g.get('second_server_points_on_serve_won'),
                    'second_server_points_on_receive_won': g.get('second_server_points_on_receive_won')
                })
                all_dim_pbp_tiebreak_lines.add(g['server_game_points_line'])
            pbp_games_to_db.append(
                {'t24_match_id': g.get('t24_match_id'),
                 'set': g.get('set'),
                 'game': g.get('game'),
                 'server': g.get('server'),
                 'server_game_points_line': g.get('server_game_points_line') if g.get('game') < 13 else None,
                 'server_tiebreak_points_line': g.get('server_game_points_line') if g.get('game') == 13 else None
                 })
        await self.__dbo.insert_or_update_many('public', 'dim_game_pbp', new_dim_pbp_game_lines_to_db,
                                               ['server_points_line'])
        await self.__dbo.insert_or_update_many('public', 'dim_tiebreak_pbp', new_dim_pbp_tiebreak_lines_to_db,
                                               ['server_points_line'])
        await self.__dbo.insert_or_update_many('public', 't24_game_pbp', pbp_games_to_db,
                                               ['t24_match_id', 'set', 'game'])

    async def get_match_statistic_by_match_id(self, t24_match_id: str) -> list:
        url = f'https://global.flashscore.ninja/107/x/feed/df_st_2_{t24_match_id}'
        data = await super()._get_html_async(url, need_soup=False)
        if not data:
            return []
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
        section, period, key_out, t1_val, t2_val, period_last, = None, None, None, None, None, None
        statistic = {'t1': {'t24_match_id': t24_match_id,
                            'team_num': 1,
                            'set': period},
                     't2': {'t24_match_id': t24_match_id,
                            'team_num': 2,
                            'set': period}}
        for part in data:
            parts_of_part = part.split('¬')
            period_last = period
            if part[:2] == 'SE':
                period = parts_of_part[0].split('÷')[1]
                try:
                    period = 0 if period == 'Match' else int(period.replace('Set ', ''))
                except Exception as e:
                    print(e)
                    period = None
            if period is None:
                continue
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
            if period != period_last or part[:2] == 'A1':
                if len(statistic['t1'].keys()) > 3 and len(statistic['t2'].keys()) > 3:
                    match_statistic.append(statistic['t1'])
                    match_statistic.append(statistic['t2'])
                if part[:2] != 'A1':
                    statistic = {'t1': {'t24_match_id': t24_match_id,
                                        'team_num': 1,
                                        'set': period},
                                 't2': {'t24_match_id': t24_match_id,
                                        'team_num': 2,
                                        'set': period}}
        return match_statistic


if __name__ == '__main__':
    import dbo
    import asyncio

    dbo = dbo.DBOperator()
    t24 = T24Matches(dbo)
    asyncio.run(t24.get_match_statistic_by_match_id('6kHSSC8L'))
