import asyncio
from datetime import datetime, timezone
import json
import re
import urllib.error
import urllib.request as ulr

from bs4 import BeautifulSoup

from dags_modules.dbo import DBOperator as dbo
from dags_modules.t24_match_pbp_parser import T24matchPBPparser


class Tennis24:
    def __init__(self):
        self._concurrency = 50
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._dbo = dbo()
        self._all_player_ids = set()
        self._new_players = []
        self._bpb_parser = T24matchPBPparser()

    async def _get_html_async(self, page_url: str, need_soup: bool = True) -> BeautifulSoup | str | None:
        def fetch_html(url: str):
            req = ulr.Request(url, headers={'x-fsign': 'SW9D1eZo'})
            with ulr.urlopen(req) as response:
                return response.read().decode('utf-8')

        async with self._semaphore:
            while True:
                try:
                    # print('Getting from:', page_url)
                    html = await asyncio.to_thread(fetch_html, page_url.replace('-/-', '--'))
                    break
                except urllib.error.HTTPError as http_er:
                    if http_er.code == 404:
                        return
                    else:
                        print(f'!!!!! T24 html loading failed. URL: {page_url}\nError:', http_er)
                        await asyncio.sleep(3)
                except Exception as e:
                    print(f'!!!!! T24 html loading failed. URL: {page_url}\nError:', e)
                    await asyncio.sleep(3)
        if need_soup:
            return BeautifulSoup(html, 'lxml')
        return html

    async def _get_url_redirect_endpoint(self, page_url: str) -> str | None:
        def fetch_redirect(url: str):
            req = ulr.Request(url, headers={'x-fsign': 'SW9D1eZo'})
            with ulr.urlopen(req) as response:
                return response.geturl()

        async with self._semaphore:
            while True:
                try:
                    final_url = await asyncio.to_thread(fetch_redirect, page_url)
                    return final_url
                except urllib.error.HTTPError as http_er:
                    if http_er.code in (301, 302, 303, 307, 308):
                        return http_er.headers.get("Location")
                    elif http_er.code == 404:
                        return None
                    else:
                        print(f'!!!!! Redirect check failed. URL: {page_url}\nError:', http_er)
                        await asyncio.sleep(3)
                except Exception as e:
                    print(f'!!!!! Redirect check failed. URL: {page_url}\nError:', e)
                    await asyncio.sleep(3)

    async def _load_all_players_from_db(self):
        players = await self._dbo.select('public', 't24_players', ['t24_pl_id'], where_conditions=None)
        self._all_player_ids = {player['t24_pl_id'] for player in players}

    async def _get_initial_match_data_by_t24_match_id(self, match_players: dict) -> dict | None:
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
                    if pl['id'] not in self._all_player_ids:
                        pl_full_name, birthday = await self.__get_t24_pl_full_data(pl['id'])
                        self._new_players.append({'t24_pl_id': pl['id'],
                                                  'pl_url': f'https://www.tennis24.com{pl['detail_link']}',
                                                  'pl_name_short': pl['name'],
                                                  'pl_full_name': pl_full_name,
                                                  'country': pl['country'],
                                                  'birthday': birthday})
                        self._all_player_ids.add(pl['id'])
                    match_players_ids_out.update({f'{team_db}_pl{pl_num}_id': pl['id']})
        return match_players_ids_out


