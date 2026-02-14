import asyncio
import re
from bs4 import BeautifulSoup
from datetime import date
from dags_modules_atp.atp_dbo import DBOATP
from dags_modules_atp.atp_init import ATPInit


class ATPTournaments(ATPInit):
    def __init__(self, dbo_in: DBOATP):
        super().__init__()
        self.__dbo = dbo_in
        self.__all_tournament_ids = set()

    # async def init_async(self):
    #     await self.__get_db_all_tournament_ids()
    #
    # async def __get_db_all_tournament_ids(self):
    #     trn_ids = await self.__dbo.select('public', 'atp_tournaments', ['atp_trn_id'])
    #     trn_ids = [p['atp_pl_id'] for p in trn_ids]
    #     self.__all_tournament_ids.update(trn_ids)

    async def get_one_year_result_archive(self, year_in: int):
        months_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6, 'July': 7, 'August': 8,
                       'September': 9, 'October': 10, 'November': 11, 'December': 12}
        tour_type_dict = {'atpgs': 'A', 'ch': 'C'}
        atp_tournaments_from_calendar_out = []
        for tour_type in (tour_type_dict.keys()):
            url = 'https://www.atptour.com/en/scores/results-archive?year=' + \
                  str(year_in) + '&tournamentType=' + tour_type
            soup = await self._get_html_async_curl_cffi(url)
            trns_list = soup.find('div', class_='tournament-list')
            trns_bs4 = trns_list.find_all('ul', class_='events')
            for trn_bs4 in trns_bs4:
                tournament_profile = trn_bs4.find('a', class_='tournament__profile')
                trn_atp_link = tournament_profile.get('href').split('/')[-2]
                atp_trn_name = tournament_profile.find('span', class_='name').text
                trn_location = tournament_profile.find('span', class_='venue').text.replace(' | ', '').split(', ')
                trn_city = trn_location[0] if trn_location[0].strip() != '' else None
                trn_country = trn_location[1] if trn_location[1].strip() != '' else None
                dates_raw = tournament_profile.find('span', class_='Date').text.split(' - ')
                dates_dict = {}
                for i in range(len(dates_raw)):
                    date_type = 'start_date' if i == 0 else 'end_date'
                    date_split_comma = dates_raw[i].split(', ')
                    year = int(date_split_comma[1]) if len(date_split_comma) > 1 else None
                    day_and_month = date_split_comma[0].split()
                    day = int(day_and_month[0])
                    month = months_dict[day_and_month[1]] if len(day_and_month) > 1 else None
                    dates_dict[date_type] = {'day': day, 'month': month, 'year': year}
                if dates_dict['start_date']['month'] is None:
                    dates_dict['start_date']['month'] = dates_dict['end_date']['month']
                if dates_dict['start_date']['year'] is None:
                    dates_dict['start_date']['year'] = dates_dict['end_date']['year']
                start_date = date(dates_dict['start_date']['year'],
                                  dates_dict['start_date']['month'],
                                  dates_dict['start_date']['day'])
                end_date = date(dates_dict['end_date']['year'],
                                dates_dict['end_date']['month'],
                                dates_dict['end_date']['day'])
                atp_tournaments_from_calendar_out.append({'atp_trn_id': trn_atp_link,
                                                          'trn_year': year_in,
                                                          'tour_type': tour_type_dict[tour_type],
                                                          'trn_name': atp_trn_name,
                                                          'trn_start_date': start_date,
                                                          'trn_end_date': end_date,
                                                          'trn_city': trn_city,
                                                          'trn_country': trn_country})
        return atp_tournaments_from_calendar_out




if __name__ == '__main__':
    atp = ATPTournaments(None)
    asyncio.run(atp.load_results_matches({'atp_trn_id': '540', 'trn_year': 2010, 'trn_start_date': date(2010, 6, 21),
                                          'draws_count': None, 'singles_main_draw_loaded': None,
                                          'singles_qualification_draw_loaded': None, 'doubles_main_draw_loaded': None,
                                          'doubles_qualification_draw_loaded': None, 'results_loaded': None}))
    # asyncio.run(atp.get_one_year_result_archive(2024))
    # atp.check_rating_date_in_db('2024-01-01')
    # asyncio.run(atp.get_tournament_by_url('https://www.atptour.com/en/-/tournaments/explore/2'))
