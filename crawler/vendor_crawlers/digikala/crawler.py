import asyncio
import aiohttp

from datetime import datetime
from typing import List, Dict, Any

from starlette import status

from crawler.vendor_crawlers import VendorCrawlerAbstract
from crawler.databases import MongoStorage
from crawler.clrawlerstate import CrawlerState


class DigikalaCrawler(VendorCrawlerAbstract):

    def __init__(self):
        self.name = 'digikala'
        self.base_url = 'https://api.digikala.com'
        self.mongo_storage = MongoStorage()
        self.data_collection_name = f'data_{self.name}'
        self.time_collection_name = f'time_{self.name}'

    async def create_links(self, categorize: str, min_page=1, max_page=2) -> List[str]:
        list_urls = [f'/v1/categories/{categorize}/search/?page={i}' for i in
                     range(min_page, min(max_page, 10))]

        return list_urls

    async def downloader(self, url: str, *args, **kwargs) -> Dict:
        async with aiohttp.ClientSession(base_url=self.base_url) as session:
            async with session.request(method='GET', url=url) as response:

                if response.status == status.HTTP_200_OK:
                    result = await response.json()  # JSON Response Content
                elif response.status == status.HTTP_403_FORBIDDEN:
                    raise ValueError('Error! Please check your request headers')
                elif response.status == status.HTTP_429_TOO_MANY_REQUESTS:
                    result = {'data': {'products': []}}
                else:
                    raise ValueError('Website is down Or you are offline')

        return result

    async def parser(self, product: dict, *args, **kwargs) -> Dict:
        id = product['id']
        title_fa = product['title_fa']
        category = product['data_layer']['category']
        category = category[4:-1]
        exist = len(product['default_variant']) != 0
        if exist:
            rrp_price = product['default_variant']['price']['rrp_price']
            selling_price = product['default_variant']['price']['selling_price']
        else:
            rrp_price = None
            selling_price = None

        data = {
            'id': id,
            'title': title_fa,
            'category': category,
            'exist': exist,
            'rrp_price': rrp_price,
            'selling_price': selling_price,
        }

        return data

    async def run(self) -> None:
        information = [
            {
                'categorize': 'dairy',
                'max_page': 30,
            },
            {
                'categorize': 'protein-foods',
                'max_page': 19,
            },
            {
                'categorize': 'groceries',
                'max_page': 95,
            },
        ]

        ts = datetime.now()
        list_urls = await asyncio.gather(
            *[self.create_links(categorize=info['categorize'], max_page=info['max_page']) for info in information])
        await self.record_report(time_spent=str(datetime.now() - ts), state=str(CrawlerState.phase_create_links.value))

        ts = datetime.now()
        # Download information
        results = []
        for list_url in list_urls:
            results += await asyncio.gather(
                *[self.downloader(url=url) for url in list_url])
        await self.record_report(time_spent=str(datetime.now() - ts),
                                 state=str(CrawlerState.phase_download_links.value))

        ts = datetime.now()
        # Pars information
        data = []
        for result in results:
            data += await asyncio.gather(
                *[self.parser(product=product) for product in result['data']['products']])
        await self.record_report(time_spent=str(datetime.now() - ts), state=str(CrawlerState.phase_pars_data.value))

        # Store parsed data in mongoDB
        ts = datetime.now()
        await self.mongo_storage.store(data=data, collection=self.data_collection_name)
        await self.record_report(time_spent=str(datetime.now() - ts), state=str(CrawlerState.phase_save_data.value))

    async def record_report(self, time_spent: str, state: str):
        data = {
            'date_time': str(datetime.now()),
            'time_spent': time_spent,
            'state': state,
        }
        await self.mongo_storage.store(data=data, collection=self.time_collection_name)
