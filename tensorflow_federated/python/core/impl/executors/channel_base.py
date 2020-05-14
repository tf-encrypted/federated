import abc


class Channel(metaclass=abc.ABCMeta):

  @abc.abstractmethod
  async def send(self, value, sender=None, receiver=None):
    pass

  @abc.abstractmethod
  async def receive(self, value, sender=None, receiver=None):
    pass

  @abc.abstractmethod
  async def setup(self):
    pass
