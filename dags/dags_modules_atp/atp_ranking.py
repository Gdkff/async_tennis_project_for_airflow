import asyncio
from datetime import datetime, date
from dags_modules_atp.atp_dbo import DBOATP
from dags_modules_atp.atp_init import ATPInit


class ATPRanking(ATPInit):
    def __init__(self, dbo_in: DBOATP):
        super().__init__()
        self.__dbo = dbo_in
        self.db_dates = {}

    async def init_async(self):
        await self.__db_ranking_dates_loading()

    async def __db_ranking_dates_loading(self):
        for ranking_type in ('singles', 'doubles'):
            ranking_type_to_db = 'S' if ranking_type == 'singles' else 'D'
            dates = await self.__dbo.select('public', 'atp_ranking_dates', ['week_date'],
                                            {'ranking_type': ranking_type_to_db})
            dates = [d['week_date'] for d in dates]
            self.db_dates[ranking_type] = dates
        print('Даты рейтинга загружены в кэш')

    async def web_ranking_dates_loading(self, ranking_type: str) -> list[date]:
        url = f'https://www.atptour.com/en/rankings/{ranking_type}'
        page_soup = await self._get_html_async(url, need_soup=True)
        dates_filter = page_soup.find('select', {'class': 'joiner trigger-change', 'id': 'dateWeek-filter'})
        dates_soup = dates_filter.find_all('option')
        dates = [datetime.strptime(rating_date.text, "%Y.%m.%d").date() for rating_date in dates_soup[::-1]]
        return dates

    async def get_ranking_by_date_and_type(self, ranking_type_full: str, week_date: date) -> dict:
        ranking_type_short_for_db = 'S' if ranking_type_full == 'singles' else 'D'
        url = f'https://www.atptour.com/en/rankings/{ranking_type_full}?dateWeek={week_date}&rankRange=0-5000'
        # print(url, 'начало загрузки')
        page_soup = await self._get_html_async_curl_cffi(url, need_soup=True)
        ranking_table_soup = page_soup.find('table', class_='mega-table desktop-table non-live')
        if not ranking_table_soup:
            print('No soup #1', url)
            date_ranking_data = {
                'rank': None,
                'date': {'ranking_type': ranking_type_short_for_db, 'week_date': week_date, 'url': url,
                         'count_rows_on_page': None, 'count_rows_loaded': None, 'loaded': False}}
            return date_ranking_data
        soup_lines = ranking_table_soup.find_all('tr')[1:]
        count_rows_on_page = len(soup_lines)
        if not soup_lines:
            # print(ranking_table_soup)
            print('No soup #2', url)
            date_ranking_data = {
                'rank': None,
                'date': {'ranking_type': ranking_type_short_for_db, 'week_date': week_date, 'url': url,
                         'count_rows_on_page': count_rows_on_page, 'count_rows_loaded': None, 'loaded': False}}
            return date_ranking_data
        ranking_data = []
        for line_soup in soup_lines:
            rank = line_soup.find('td', class_='rank bold heavy tiny-cell')
            if not rank:
                continue
            rank = rank.text.replace('T', '')
            player_name_soup = line_soup.find('li', class_='name center')
            atp_pl_id = player_name_soup.find('a').get('href').split('/')[-2]
            points = (line_soup.find('td', class_="points center bold extrabold small-cell").find('a').
                      text.strip()).replace(',', '')
            tournaments_played = line_soup.find('td', class_='tourns center small-cell').text.strip()
            tournaments_played = tournaments_played if tournaments_played.isnumeric() else 0
            ranking_data.append({'week_date': week_date,
                                 'rank': int(rank),
                                 'atp_pl_id': atp_pl_id,
                                 'points': int(points),
                                 'trn_played': int(tournaments_played)})
        count_rows_loaded = len(ranking_data)
        date_ranking_data = {
            'rank': ranking_data,
            'date': {'ranking_type': ranking_type_short_for_db, 'week_date': week_date, 'url': url,
                     'count_rows_on_page': count_rows_on_page, 'count_rows_loaded': count_rows_loaded,
                     'loaded': True}}
        # print(url, 'окончание загрузки')
        return date_ranking_data


if __name__ == '__main__':
    atp = ATPRanking(None)
    asyncio.run(atp.get_ranking_by_date_and_type('singles', date(1973, 8, 23)))
