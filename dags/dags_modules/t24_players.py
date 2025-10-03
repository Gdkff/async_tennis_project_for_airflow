from dags_modules.t24_init import Tennis24, asyncio
from datetime import datetime, date, timedelta
from settings.config import tz
import re
import json


class T24Players(Tennis24):
    def __init__(self):
        super().__init__()
        self.all_players = {}
        self.t24_trn = None

    async def init_async(self):
        await self._dbo.init_pool()
        await self.__get_db_players()

    async def __get_db_players(self):
        players = await self._dbo.select('public', 't24_players', ['t24_pl_id'])
        self.all_players = {trn['t24_pl_id'] for trn in players}

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
                    if pl['id'] not in self.all_players:
                        pl_full_name, birthday = await self.__get_t24_pl_full_data(pl['id'])
                        self._new_players.append({'t24_pl_id': pl['id'],
                                                  'pl_url': f'https://www.tennis24.com{pl['detail_link']}',
                                                  'pl_name_short': pl['name'],
                                                  'pl_full_name': pl_full_name,
                                                  'country': pl['country'],
                                                  'birthday': birthday})
                        self.all_players.append(pl['id'])
                    match_players_ids_out.update({f'{team_db}_pl{pl_num}_id': pl['id']})
        return match_players_ids_out
