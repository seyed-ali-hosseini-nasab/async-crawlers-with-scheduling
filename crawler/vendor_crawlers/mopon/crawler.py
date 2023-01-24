import asyncio
import aiohttp
from starlette import status
from datetime import datetime
from typing import List
from crawler.vendor_crawlers import VendorCrawlerAbstract
from crawler.databases import MongoStorage
from crawler.clrawlerstate import CrawlerState


class MoponCrawler(VendorCrawlerAbstract):
    def __init__(self):
        self.name = 'mopon'
        self.base_url = 'http://www.application.mopon.ir'
        self.mongo_storage = MongoStorage()
        self.data_collection_name = f'data_{self.name}'
        self.time_collection_name = f'time_{self.name}'

    async def create_links(self) -> List[str]:
        # Get the total number of pages
        first_link = '/api/coupon/get?category_id=JBGDg&order_by=newest&page=1'
        information: List[dict] = await self.downloader(links=[first_link])
        last_page = information[0]['data']['last_page']
        links = [f'/api/coupon/get?category_id=JBGDg&order_by=newest&page={i}' for i in
                 range(1, last_page + 1)]

        # Get all IDs of coupons
        information: List[dict] = await self.downloader(links=links)
        coupon_ids = []
        for inf in information:
            for data in inf['data']['data']:
                coupon_ids.append(data['id'])

        # Creating links to download coupons
        result = [f'/api/coupon/find/{coupon_id}' for coupon_id in coupon_ids]
        return result

    async def downloader(self, links: list) -> List[dict]:
        connector = aiohttp.TCPConnector(limit=20)  # limit < 29
        async with aiohttp.ClientSession(base_url=self.base_url, trust_env=True, connector=connector) as session:
            if not isinstance(links, list):
                raise ValueError('links is not a list')

            async def get(link: str) -> dict:
                async with session.get(url=link, ssl=False) as response:
                    if response.status == status.HTTP_200_OK:
                        data = await response.json()  # JSON Response Content
                    elif response.status == status.HTTP_403_FORBIDDEN:
                        raise ValueError('Error! Please check your request headers')
                    elif response.status == status.HTTP_429_TOO_MANY_REQUESTS:
                        pass
                    else:
                        raise ValueError('Website is down Or you are offline')
                return data

            result = await asyncio.gather(*[get(link=link) for link in links])
        return result

    async def parser(self, data: dict) -> dict:
        data_parsed = {
            "id": data["id"],
            "name": data["name"],
            "title": data["title"],
            "code": data["code"],
        }

        return data_parsed

    async def run(self):
        ts = datetime.now()
        # Creating links for download
        links = await self.create_links()
        await self.record_report(time_spent=str(datetime.now() - ts), state=str(CrawlerState.phase_create_links.value))

        ts = datetime.now()
        # Download links
        information = await self.downloader(links=links)
        await self.record_report(time_spent=str(datetime.now() - ts),
                                 state=str(CrawlerState.phase_download_links.value))

        ts = datetime.now()
        # Pars information
        data_parsed = await asyncio.gather(*[self.parser(data=inf['data']) for inf in information])
        await self.record_report(time_spent=str(datetime.now() - ts), state=str(CrawlerState.phase_pars_data.value))

        # Store parsed data in mongoDB
        ts = datetime.now()
        await self.mongo_storage.store(data=data_parsed, collection=self.data_collection_name)
        await self.record_report(time_spent=str(datetime.now() - ts), state=str(CrawlerState.phase_save_data.value))

    async def record_report(self, time_spent: str, state: str):
        data = {
            'date_time': str(datetime.now()),
            'time_spent': time_spent,
            'state': state,
        }
        await self.mongo_storage.store(data=data, collection=self.time_collection_name)
