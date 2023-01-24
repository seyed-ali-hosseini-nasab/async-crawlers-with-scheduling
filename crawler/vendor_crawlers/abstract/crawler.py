from abc import abstractmethod, ABC


class VendorCrawlerAbstract(ABC):
    @abstractmethod
    async def create_links(self, *args, **kwargs):
        pass
    @abstractmethod
    async def downloader(self, *args, **kwargs):
        pass

    @abstractmethod
    async def parser(self, *args, **kwargs):
        pass

    @abstractmethod
    async def run(self):
        pass
