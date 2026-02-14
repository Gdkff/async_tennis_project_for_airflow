import asyncio
import json
from datetime import datetime
from dags_modules_atp.atp_dbo import DBOATP
from dags_modules_atp.atp_init import ATPInit


class ATPPlayers(ATPInit):
    def __init__(self, dbo_in: DBOATP):
        super().__init__()
        self.__dbo = dbo_in
        self.all_pl_ids = set()

    async def init_async(self):
        await self.__db_all_pl_ids_loading()

    async def __db_all_pl_ids_loading(self):
        pl_ids = await self.__dbo.select('public', 'atp_players', ['atp_pl_id'])
        self.all_pl_ids = {p['atp_pl_id'] for p in pl_ids}
        print('Игроки загружены в кэш')

    async def get_pl_info_by_atp_pl_id(self, atp_pl_id: str) -> dict | None:
        url = f'https://www.atptour.com/en/-/www/players/hero/{atp_pl_id}'
        pl_data = await self._get_html_async(url, need_soup=False)
        if pl_data:
            pl_data = json.loads(pl_data)
        else:
            print(f'Player not found: {atp_pl_id}')
            return None
        if pl_data is None:
            return {'atp_pl_id': atp_pl_id,
                    'url': None,
                    'last_name': None,
                    'first_name': None,
                    'country': None,
                    'birthday': None,
                    'birthplace': None,
                    'weight': None,
                    'height': None,
                    'play_hand': None,
                    'back_hand': None,
                    'coach': None,
                    'pl_data_loaded': False}
        pl_data = {'atp_pl_id': atp_pl_id,
                   'url': f'https://www.atptour.com/en/players/-/{atp_pl_id}/overview',
                   'last_name': pl_data.get('LastName'),
                   'first_name': pl_data.get('FirstName'),
                   'country': pl_data.get('Nationality'),
                   'birthday': datetime.strptime(pl_data['BirthDate'],
                                                 '%Y-%m-%dT%H:%M:%S') if pl_data.get('BirthDate') else None,
                   'birthplace': pl_data.get('BirthCity'),
                   'weight': pl_data.get('WeightKg'),
                   'height': pl_data.get('HeightCm'),
                   'play_hand': pl_data['PlayHand']['Id'] if pl_data.get('PlayHand') else None,
                   'back_hand': pl_data['BackHand']['Id'] if pl_data.get('BackHand') else None,
                   'coach': pl_data.get('Coach'),
                   'pl_data_loaded': True}
        return pl_data

    async def find_real_new_players_in_list(self, players: set) -> list:
        new_players = [pl for pl in players if pl not in self.all_pl_ids]
        return new_players


if __name__ == '__main__':
    atp = ATPPlayers(None)
    asyncio.run(atp.get_pl_info_by_atp_pl_id('c172'))
    # atp.check_rating_date_in_db('2024-01-01')
    # response = requests.get('https://www.atptour.com/en/-/www/activity/sgl/mm58/', params=params, cookies=cookies, headers=headers)
