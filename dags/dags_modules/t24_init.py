import urllib.error
import urllib.request as ulr
import asyncio
from bs4 import BeautifulSoup
from dags_modules.dbo import DBOperator as dbo
from dags_modules.t24_match_pbp_parser import T24matchPBPparser


class Tennis24:
    def __init__(self):
        self._concurrency = 50
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._dbo = dbo()
        self._players = []
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
