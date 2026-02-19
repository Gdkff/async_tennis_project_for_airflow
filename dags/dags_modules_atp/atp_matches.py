from bs4 import BeautifulSoup
from bs4.element import PageElement
from datetime import date
from dags_modules_atp.atp_dbo import DBOATP
from dags_modules_atp.atp_init import ATPInit


class ATPMatches(ATPInit):
    def __init__(self, dbo_in: DBOATP):
        super().__init__()
        self.__dbo = dbo_in
        self.__all_tournament_ids = set()
        self.__bye_players = {'v442'}

    async def __get_draw_soup_and_draw_types_list(self, atp_trn_id: str, trn_year: int, draw_type: str,
                                                  get_draw_types_list: bool = False) -> (BeautifulSoup | None,
                                                                                         list[str] | None):
        url = f'https://www.atptour.com/en/scores/archive/a/{atp_trn_id}/{str(trn_year)}/draws?matchType={draw_type}'
        print(url)
        page_soup = await self._get_html_async_curl_cffi(url)
        draw_soup = page_soup.find('div', class_='atp_draw')
        if get_draw_types_list:
            if not draw_soup:
                return [], []
            tab_switcher = draw_soup.find('div', class_='tab-switcher')
            if not tab_switcher:
                return [], []
            raw_match_types = tab_switcher.find_all('a')
            if not raw_match_types or len(raw_match_types) == 0:
                return [], []
            draw_types_list = []
            for raw_match_type in raw_match_types:
                # print(str(raw_match_type))
                if 'https://www.protennislive.com/' not in str(raw_match_type):
                    draw_types_list.append(raw_match_type.get('href').split('=')[1])
        else:
            draw_types_list = None
        if not draw_soup:
            return [], draw_types_list
        draw_soup = draw_soup.find_all('div', class_='atp-draw-container')[-1]
        draw_headers = draw_soup.find_all('div', class_="draw-header")
        draw_headers = [draw_header.text.strip() for draw_header in draw_headers]
        draw_contents = draw_soup.find_all('div', class_="draw-content")
        draws_data = zip(draw_headers, draw_contents)
        return draws_data, draw_types_list

    def __parse_match_soup(self, match_soup: BeautifulSoup | PageElement) -> dict | None:
        match_stats_teg = match_soup.find('a', string='Stats')
        atp_match_id = match_stats_teg.get('href').split('/')[-1] if match_stats_teg else None
        match_data = {
            'atp_match_id': atp_match_id,
            't1_pl1_id': None,
            't1_pl1_seed': None,
            't1_pl1_info': None,
            't1_pl2_id': None,
            't1_pl2_seed': None,
            't1_pl2_info': None,
            't2_pl1_id': None,
            't2_pl1_seed': None,
            't2_pl1_info': None,
            't2_pl2_id': None,
            't2_pl2_seed': None,
            't2_pl2_info': None,
            'team_winner': None,
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
            't2_s5_score_tiebreak': None
        }
        teams = match_soup.find_all('div', class_='stats-item')
        players_set = set()
        for team_num, team in enumerate(teams, 1):
            winner = team.find('div', class_='winner')
            if winner:
                match_data['team_winner'] = team_num
            plrs = team.find_all('div', class_='name')
            for plr_num, plr in enumerate(plrs, 1):
                atp_pl_id = plr.find('a').get('href').split('/')[4] \
                    if plr.find('a') and len(plr.find('a').get('href').split('/')) == 6 else None
                if atp_pl_id == '0':
                    atp_pl_id = None
                players_set.add(atp_pl_id)
                pl_info = plr.find('span').text.replace('(', '').replace(')', '') \
                    if plr.find('span') else None
                seed, info = None, None
                if pl_info:
                    if pl_info.isdigit():
                        seed = int(pl_info)
                    else:
                        if ' ' in pl_info:
                            seed = int(pl_info.split(' ')[0])
                            info = pl_info.split(' ')[1]
                        else:
                            info = pl_info
                match_data.update({
                    f't{team_num}_pl{plr_num}_id': atp_pl_id if atp_pl_id not in self.__bye_players else None,
                    f't{team_num}_pl{plr_num}_seed': seed,
                    f't{team_num}_pl{plr_num}_info': info})
            set_scores = team.find_all('div', class_='score-item')
            for set_num, set_score in enumerate(set_scores):
                score = set_score.find_all('span')
                set_score = score[0].text if score and score[0].text != '-' else None
                set_tb_score = score[1].text if len(score) > 1 and score[1].text != '-' else None
                match_data.update({
                    f't{team_num}_s{set_num + 1}_score': int(
                        set_score) if set_score is not None and set_score.isdigit() else None,
                    f't{team_num}_s{set_num + 1}_score_tiebreak': int(
                        set_tb_score) if set_tb_score is not None and set_tb_score.isdigit() else None})
        if players_set == {None}:
            return None
        return match_data

    async def __load_draw_matches(self, trn_data: dict):
        draw_names_dict = {'singles': 'singles_main_draw_matches',
                           'qualifiersingles': 'singles_qualification_draw_matches',
                           'doubles': 'doubles_main_draw_matches',
                           'qualifierdoubles': 'doubles_qualification_draw_matches'}
        singles_draw_soup, draw_types_list = await self.__get_draw_soup_and_draw_types_list(trn_data['atp_trn_id'],
                                                                                            trn_data['trn_year'],
                                                                                            'singles',
                                                                                            get_draw_types_list=True)
        trn_data['draws_count'] = len(draw_types_list)
        if trn_data['draws_count'] == 0:
            trn_data['matches_loaded'] = False
            return trn_data, []
        draw_soups = {'singles': singles_draw_soup}
        draw_types_list.remove('singles')
        for draw_type in draw_types_list:
            draw_soup, _ = await self.__get_draw_soup_and_draw_types_list(trn_data['atp_trn_id'], trn_data['trn_year'],
                                                                          draw_type)
            draw_soups[draw_type] = draw_soup
        matches_out = []
        for draw_name, soup in draw_soups.items():
            draw_matches = []
            for trn_round_num, trn_round in enumerate(soup, 1):
                trn_round_name, trn_round_soup = trn_round
                matches = trn_round_soup.find_all('div', class_='draw-item') if round is not None else None
                if not matches:
                    continue
                for match_num, match_soup in enumerate(matches, 1):
                    match_data = {
                        'atp_trn_id': trn_data['atp_trn_id'],
                        'trn_year': trn_data['trn_year'],
                        'trn_start_date': trn_data['trn_start_date'],
                        'draw_name': draw_name,
                        'trn_round_name': trn_round_name,
                        'round_number': trn_round_num,
                        'match_number': match_num,
                        'match_duration': None,
                        'exists_in_draw': True,
                        'exists_in_results': None
                    }
                    match_data_from_soup_parsing = self.__parse_match_soup(match_soup)
                    if match_data_from_soup_parsing is not None:
                        match_data.update(match_data_from_soup_parsing)
                        match_data['match_stats_url'] = \
                            (
                                f'https://www.atptour.com/en/scores/match-stats/archive/{trn_data['trn_year']}/'
                                f'{trn_data['atp_trn_id']}/{match_data['atp_match_id']}'
                            ) if match_data['atp_match_id'] else None
                        draw_matches.append(match_data)
            trn_data[draw_names_dict[draw_name]] = len(draw_matches)
            matches_out += draw_matches
        trn_data['matches_loaded'] = True
        return trn_data, matches_out

    async def __load_results_matches(self, trn_data: dict):
        matches_out = []
        for trn_type in ('singles', 'doubles'):
            url = (f'https://www.atptour.com/en/scores/archive/_/{trn_data['atp_trn_id']}/{trn_data['trn_year']}'
                   f'/results?matchType={trn_type}')
            page_soup = await self._get_html_async_curl_cffi(url)
            atp_accordion_items = page_soup.find('div', class_='atp_accordion-items')
            if not atp_accordion_items:
                trn_data['matches_loaded'] = False
                return trn_data, []
            trn_round_soups = atp_accordion_items.find_all('div', class_='atp_accordion-item')
            trn_round_num = 0
            qualifying_last = True
            for trn_round_soup in trn_round_soups[::-1]:
                trn_round_num += 1
                match_soups = trn_round_soup.find_all('div', class_='match')
                for match_num, match_soup in enumerate(match_soups, 1):
                    match_header_soups = match_soup.find_all('span')
                    match_header = [m.text for m in match_header_soups]
                    qualifying_current = 'Qualifying' in match_header[0][:-3]
                    trn_round_num = 1 if qualifying_current != qualifying_last else trn_round_num
                    qualifying_last = qualifying_current
                    match_data = {
                        'atp_trn_id': trn_data['atp_trn_id'],
                        'trn_year': trn_data['trn_year'],
                        'trn_start_date': trn_data['trn_start_date'],
                        'draw_name': 'qualifier' + trn_type if qualifying_current else trn_type,
                        'trn_round_name': match_header[0][:-3],
                        'round_number': trn_round_num,
                        'match_number': match_num,
                        'match_duration': match_header[1],
                        'exists_in_draw': None,
                        'exists_in_results': True
                    }
                    match_data_from_soup_parsing = self.__parse_match_soup(match_soup)
                    if match_data_from_soup_parsing is not None:
                        match_data.update(match_data_from_soup_parsing)
                        match_data['match_stats_url'] = \
                            (
                                f'https://www.atptour.com/en/scores/match-stats/archive/{trn_data['trn_year']}/'
                                f'{trn_data['atp_trn_id']}/{match_data['atp_match_id']}'
                            ) if match_data['atp_match_id'] else None
                        matches_out.append(match_data)
        trn_data['matches_loaded'] = True
        return trn_data, matches_out

    @staticmethod
    def __operate_draws_and_results_matches(matches_from_draws: list[dict],
                                            matches_from_results: list[dict]) -> (list[dict], set):
        matches_out = []
        matches_with_pl_frozenset_as_key = {}
        players_set = set()
        for m in matches_from_draws:
            pl_lines_from_draw_matches = frozenset(x for x in [m['t1_pl1_id'], m['t1_pl2_id'], m['t2_pl1_id'],
                                                               m['t2_pl2_id']] if x is not None)
            players_set.update(pl_lines_from_draw_matches)
            matches_with_pl_frozenset_as_key[pl_lines_from_draw_matches] = {'draws': m}
        for m in matches_from_results:
            pl_lines_from_results_matches = frozenset(x for x in [m['t1_pl1_id'], m['t1_pl2_id'], m['t2_pl1_id'],
                                                                  m['t2_pl2_id']] if x is not None)
            players_set.update(pl_lines_from_results_matches)
            if matches_with_pl_frozenset_as_key.get(pl_lines_from_results_matches) is None:
                matches_with_pl_frozenset_as_key[pl_lines_from_results_matches] = {'results': m}
            else:
                matches_with_pl_frozenset_as_key[pl_lines_from_results_matches]['results'] = m
        if 'Round Robin' in str(matches_from_results):
            return matches_from_results, players_set
        for key, match_from_draws_and_results in matches_with_pl_frozenset_as_key.items():
            if len(match_from_draws_and_results) < 2:
                for match_data in match_from_draws_and_results.values():
                    matches_out.append(match_data)
            else:
                match_from_draws_and_results['draws']['match_duration'] \
                    = match_from_draws_and_results['results']['match_duration']
                match_from_draws_and_results['draws']['exists_in_results'] \
                    = match_from_draws_and_results['results']['exists_in_results']
                matches_out.append(match_from_draws_and_results['draws'])
        return matches_out, players_set

    async def get_tournament_matches(self, trn_data: dict):
        trn_data, matches_from_draws = await self.__load_draw_matches(trn_data)
        trn_data, matches_from_results = await self.__load_results_matches(trn_data)
        matches, players = self.__operate_draws_and_results_matches(matches_from_draws, matches_from_results)
        anti_duplicate_set = set()
        for i, m in enumerate(matches):
            if (
                    m['atp_trn_id'], m['trn_year'], m['trn_start_date'], m['draw_name'],
                    m['round_number'], m['match_number']
            ) in anti_duplicate_set:
                trn_data['duplicates_in_matches'] = True
                trn_data['matches_loaded'] = False
                return trn_data, [], players
            anti_duplicate_set.add((m['atp_trn_id'], m['trn_year'], m['trn_start_date'],
                                    m['draw_name'], m['round_number'], m['match_number']))
        return trn_data, matches, players
