import unittest
from manage import Manager


class TestManager(unittest.TestCase):
    def setUp(self) -> None:
        self.manager = Manager()

    async def test_check_schedule_crawler(self):
        schedule = {
            "crawler_name": "mopon",
            "day": 0,
            "hour": 0,
            "minutes": 10,
            "seconds": 0
        }
        self.assertTrue(self.manager.check_schedule_crawler(schedule))

    async def test_get_crawlers_name_for_run(self):
        self.assertTrue(isinstance(self.manager.get_crawlers_name_for_run(), list))
        self.assertTrue('mopon' in self.manager.get_crawlers_name_for_run())

    if __name__ == '__main__':
        unittest.main()
