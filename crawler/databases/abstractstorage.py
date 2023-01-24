from abc import ABC, abstractmethod


class StorageAbstract(ABC):

    @abstractmethod
    async def store(self, data, *args, **kwargs):
        pass
