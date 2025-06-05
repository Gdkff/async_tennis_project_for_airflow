from dags_modules.t24_init import Tennis24, asyncio
from datetime import datetime, date, timedelta
from settings.config import tz


class T24DailyMatchesLoading(Tennis24):
    @staticmethod
    def __tournament_line_parsing(tournament_line: str):
        split_dict = {' - ': 'trn_category',
                      ': ': 'trn_type',
                      ' (': 'trn_name',
                      '), ': 'trn_country'}
        trn_data = {}
        for line_part in tournament_line.split('¬'):
            if line_part[:3] == 'ZA÷':
                line = line_part.split('÷')[-1]
                for splitter, key in split_dict.items():
                    split_line = line.split(splitter)
                    trn_data[key] = split_line[0]
                    line = split_line[-1]
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
                      'FL': 't2_pl_2_name',
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
                          '36': 'Interrupted'}
        match_statuses_short = {'1': 'Not started',
                                '2': 'Playing',
                                '3': 'Ended'}
        int_fields = ['team_winner', 't1_sets_won', 't2_sets_won', 't1_s1_score', 't1_s1_score_tiebreak', 't2_s1_score',
                      't2_s1_score_tiebreak', 't1_s2_score', 't1_s2_score_tiebreak', 't2_s2_score',
                      't2_s2_score_tiebreak', 't1_s3_score', 't1_s3_score_tiebreak', 't2_s3_score', 't2_s3_score_tiebreak',
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
                      't2_pl_2_name': None,
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
        match_data['match_status_short'] = match_statuses_short[match_data['match_status_short']]
        match_data['match_status'] = match_statuses[match_data['match_status']]
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
        for day_number in range(-3, 7):
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
                                       'surface': current_tournament.get('surface')})
                    matches.append(match_data)
        await self._dbo.insert_or_update_many('public', 't24_matches', matches, ['t24_match_id'])


def t24_load_daily_matches():
    t24 = T24DailyMatchesLoading()
    asyncio.run(t24.load_daily_matches())


if __name__ == '__main__':
    start_time = datetime.now()
    t24_load_daily_matches()

    print('Time length:', datetime.now() - start_time)
