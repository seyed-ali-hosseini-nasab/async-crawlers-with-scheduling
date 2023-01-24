import asyncio

from typing import List
from datetime import datetime
from vendor_crawlers import MoponCrawler, DigikalaCrawler
from databases import MongoDatabase
from clrawlerstate import CrawlerState


class Manager:

    def __get_crawlers(self) -> dict:
        monpon = MoponCrawler()
        digikala = DigikalaCrawler()

        result = {
            monpon.name: monpon,
            digikala.name: digikala,
        }

        return result

    @staticmethod
    async def get_schedules() -> List[dict]:
        mongodb = MongoDatabase()
        collection = mongodb.database['schedule']
        return [x for x in collection.find()]

    @staticmethod
    async def get_crawler_log(crawler_name: str, state: str) -> dict | None:
        mongodb = MongoDatabase()
        collection = mongodb.database[f'time_{crawler_name}']
        log = [x for x in collection.find({'state': {'$eq': state}})]
        if len(log) == 0:
            return None

        return log[-1]

    async def check_schedule_crawler(self, schedule: dict) -> bool:
        log = await self.get_crawler_log(schedule['crawler_name'], str(CrawlerState.phase_save_data.value))
        if log is None:
            return True

        crawler_finish_datetime = datetime.strptime(log['date_time'], '%Y-%m-%d %H:%M:%S.%f')
        schedule_seconds = lambda: ((schedule['day'] * 24 + schedule['hour']) * 60 +
                                    schedule['minutes']) * 60 + schedule['seconds']

        return (datetime.now() - crawler_finish_datetime).seconds >= schedule_seconds()

    async def get_crawlers_name_for_run(self) -> List[str]:
        schedules = await self.get_schedules()
        schedule_crawlers = await asyncio.gather(
            *[self.check_schedule_crawler(schedule) for schedule in schedules])
        crawlers_name = []
        for i in range(len(schedule_crawlers)):
            if schedule_crawlers[i]:
                crawlers_name.append(schedules[i]['crawler_name'])

        return crawlers_name

    async def run(self):
        crawlers_name = await self.get_crawlers_name_for_run()
        crawlers = self.__get_crawlers()
        tasks = [asyncio.create_task(crawlers[name].run()) for name in crawlers_name]
        await asyncio.gather(*tasks)


async def main():
    await Manager().run()


if __name__ == '__main__':
    import os

    if os.name == 'nt':

        from functools import wraps
        from asyncio.proactor_events import _ProactorBasePipeTransport


        def silence_event_loop_closed(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                try:
                    return func(self, *args, **kwargs)
                except RuntimeError as e:
                    if str(e) != 'Event loop is closed':
                        raise

            return wrapper


        _ProactorBasePipeTransport.__del__ = silence_event_loop_closed(_ProactorBasePipeTransport.__del__)
        policy = asyncio.WindowsSelectorEventLoopPolicy()
        asyncio.set_event_loop_policy(policy)

    asyncio.run(main())
