from dags_modules.t24_init import Tennis24, asyncio
from datetime import datetime, date, timedelta, timezone
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
        print(player_data)
        return player_data

    async def load_players_data(self):
        await self._dbo.init_pool()
        await self._dbo.close_pg_connections()
        player_ids_to_load_data = await self._dbo.select('public', 't24_players',
                                                         ['t24_pl_id'], {'all_data_loaded': None})
        player_ids_to_load_data = [pl_id['t24_pl_id'] for pl_id in player_ids_to_load_data]
        batch_size = self._concurrency
        batches = [player_ids_to_load_data[i:i + batch_size] for i in range(0, len(player_ids_to_load_data), batch_size)]
        batches_count = len(batches)
        for batch in batches:
            in_time = datetime.now()
            tasks = [self.__get_t24_pl_full_data(pl_id) for pl_id in batch]
            players = await asyncio.gather(*tasks)
            # players = [pl for inner in players if inner for pl in inner if pl]
            # print(players)
            await self._dbo.insert_or_update_many('public', 't24_players', players, ['t24_pl_id'], on_conflict_update=True)
            batches_count -= 1
            print(f'Batch processing time: {datetime.now() - in_time}. {batches_count} batches left to process.')


if __name__ == '__main__':
    t24 = T24Players()
    asyncio.run(t24.load_players_data())
    # asyncio.run(t24.get_t24_pl_full_data('CzXh4LAb'))
