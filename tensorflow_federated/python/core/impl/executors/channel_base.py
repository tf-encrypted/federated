import abc
from dataclasses import dataclass
from typing import Tuple, Dict

from tensorflow_federated.python.common_libs import py_typecheck
from tensorflow_federated.python.core.impl.compiler import placement_literals

PlacementPair = Tuple[placement_literals.PlacementLiteral,
                      placement_literals.PlacementLiteral]


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


@dataclass
class ChannelGrid:
  channel_dict: Dict[PlacementPair, Channel]

  def __getitem__(self, placements: PlacementPair):
    py_typecheck.check_type(placements, tuple)
    py_typecheck.check_len(placements, 2)
    sorted_placements = sorted(placements, key=lambda p: p.uri)
    return self.channel_dict.get(tuple(sorted_placements))
