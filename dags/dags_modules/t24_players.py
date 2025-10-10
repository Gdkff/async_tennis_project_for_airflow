import asyncpg

from dags_modules.t24_init import Tennis24, asyncio
from datetime import datetime, date, timedelta, timezone
from settings.config import tz
import re
import json


class T24Players(Tennis24):
    def __init__(self, pool: asyncpg.pool.Pool):
        super().__init__()
        self.__pool = pool
        self.__all_players = set()
        self.t24_trn = None

    async def get_db_all_players(self):
        players = await self._dbo.select(self.__pool, 'public', 't24_players', ['t24_pl_id'])
        self.__all_players = {trn['t24_pl_id'] for trn in players}

    async def get_all_new_players_from_matches(self, matches: list[dict]) -> set:
        new_players = set()
        for match in matches:
            if match['t1_pl1_id'] and match['t1_pl1_id'] not in self.__all_players:
                new_players.add(match['t1_pl1_id'])
            if match['t1_pl2_id'] and match['t1_pl2_id'] not in self.__all_players:
                new_players.add(match['t1_pl2_id'])
            if match['t2_pl1_id'] and match['t2_pl1_id'] not in self.__all_players:
                new_players.add(match['t2_pl1_id'])
            if match['t2_pl2_id'] and match['t2_pl2_id'] not in self.__all_players:
                new_players.add(match['t2_pl2_id'])
        return new_players

    async def __get_t24_pl_full_data(self, t24_pl_id: str) -> dict:
        url = await self._get_url_redirect_endpoint(f'https://www.tennis24.com/?r=4:{t24_pl_id}')
        player_soup = await self._get_html_async(url)
        birthday = None
        container__heading = player_soup.find('div', class_='container__heading')
        country = container__heading.find('span', class_='breadcrumb__text').text
        country = country if country != 'World' else None
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
        player_data = {'t24_pl_id': t24_pl_id,
                       'url': url,
                       'full_name': pl_full_name,
                       'country': country,
                       'birthday': birthday,
                       'all_data_loaded': True}
        return player_data

    async def load_players_data_to_db(self, player_ids_to_load_data: list[str]) -> list[dict]:
        batch_size = self._concurrency
        batches = [player_ids_to_load_data[i:i + batch_size] for i in range(0, len(player_ids_to_load_data), batch_size)]
        batches_count = len(batches)
        players_data_to_db = []
        for batch in batches:
            in_time = datetime.now()
            tasks = [self.__get_t24_pl_full_data(pl_id) for pl_id in batch]
            players_data_to_db += await asyncio.gather(*tasks)
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')
        return players_data_to_db



