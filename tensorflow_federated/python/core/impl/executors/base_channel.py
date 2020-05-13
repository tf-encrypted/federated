import abc


class Channel(metaclass=abc.ABCMeta):

  @abc.abstractmethod
  async def send(self, value, sender_index=None, receiver_index=None):
    pass

  @abc.abstractmethod
  async def receive(self, value, sender_index=None, receiver_index=None):
    pass

  @abc.abstractmethod
  async def setup(self):
    pass

  @abc.abstractmethod
  async def _generate_keys(self, key_owner):
    pass

  @abc.abstractmethod
  async def _share_public_keys(self, key_owner, send_pks_to):
    pass
