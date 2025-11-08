import asyncio
import urllib.error
import urllib.request as ulr

from bs4 import BeautifulSoup


class Tennis24:
    def __init__(self):
        self.concurrency = 50
        self._semaphore = asyncio.Semaphore(self.concurrency)

    async def _get_html_async(self, page_url: str, need_soup: bool = True) -> BeautifulSoup | str | None:
        def fetch_html(url: str):
            req = ulr.Request(url, headers={'x-fsign': 'SW9D1eZo'})
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
                    break
                except urllib.error.HTTPError as http_er:
                    if http_er.code == 404:
                        return None
                    else:
                        print(f'!!!!! T24 html loading failed. URL: {page_url}\nError:', http_er)
                        await asyncio.sleep(3)
                except Exception as e:
                    print(f'!!!!! T24 html loading failed. URL: {page_url}\nError:', e)
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
