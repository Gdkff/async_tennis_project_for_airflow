import asyncio
import urllib.error
import urllib.request as ulr
import urllib.parse
import random
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup


class ATPInit:
    def __init__(self):
        self.concurrency = 8
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._http_session = AsyncSession(
            impersonate="firefox",
            max_clients=self.concurrency,
            timeout=30
        )

    async def _get_html_async_curl_cffi(self, page_url: str, need_soup: bool = True) -> BeautifulSoup | str | None:
        async with self._semaphore:
            for attempt in range(1, 4):
                try:
                    resp = await self._http_session.get(
                        page_url.replace('-/-', '--'),
                        headers={
                            'Referer': 'https://www.atptour.com/en'
                        }
                    )

                    if resp.status_code == 404:
                        print('404 Error', page_url)
                        return None

                    resp.raise_for_status()
                    html = resp.text

                    # небольшая «человеческая» пауза
                    await asyncio.sleep(random.uniform(0.3, 0.9))

                    if need_soup:
                        return BeautifulSoup(html, 'lxml')
                    return html

                except Exception as e:
                    print(f'!!!!! ATP html loading failed. URL: {page_url}\nError:', e)
                    if attempt == 3:
                        return None
                    await asyncio.sleep(2)

    async def _get_html_async(self, page_url: str, need_soup: bool = True) -> BeautifulSoup | str | None:
        def fetch_html(url: str):
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.atptour.com/en',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Priority': 'u=0, i',
            }
            req = ulr.Request(url.replace('-/-', '--'), headers=headers)
            with ulr.urlopen(req) as response:
                return response.read().decode('utf-8')

        async with self._semaphore:
            counter = 0
            html = None
            while True:
                counter += 1
                try:
                    # print('Getting from:', page_url)
                    html = await asyncio.to_thread(fetch_html, page_url.replace('-/-', '--'))
                    await asyncio.sleep(random.uniform(0.3, 1.0))
                    break
                except urllib.error.HTTPError as http_er:
                    if http_er.code == 404:
                        print('404 Error', page_url)
                        return None
                    else:
                        print(f'!!!!! ATP html loading failed. URL: {page_url}\nError:', http_er)
                        await asyncio.sleep(3)
                except Exception as e:
                    print(f'!!!!! ATP html loading failed. URL: {page_url}\nError:', e)
                    await asyncio.sleep(3)
                if counter > 3:
                    break
        if not html:
            # print('no html:', page_url)
            # print(html)
            return None
        if need_soup:
            return BeautifulSoup(html, 'lxml')
        return html

    async def _get_url_redirect_endpoint(self, page_url: str) -> str | None:
        def fetch_redirect(url: str):
            req = ulr.Request(url.replace('-/-', '--'), headers={'User-Agent': 'Mozilla/5.0'})
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
