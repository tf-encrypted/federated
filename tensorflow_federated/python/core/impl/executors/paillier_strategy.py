from tensorflow_federated.python.common_libs import py_typecheck
from tensorflow_federated.python.core.api import computation_types
from tensorflow_federated.python.core.impl.executors import federating_executor

from tf_encrypted.primitives import paillier

class PaillierStrategy(federating_executor.CentralizedIntrinsicStrategy):
  def __init__(self, parent_executor):
    super().__init__(self, parent_executor)
  
  @classmethod
  def validate_executor_placements(cls, executor_placements):
    py_typecheck.check_type(executor_placements, dict)
    for k, v in executor_placements.items():
      if k is not None:
        py_typecheck.check_type(k, placement_literals.PlacementLiteral)
      py_typecheck.check_type(v, (list, executor_base.Executor))
      if isinstance(v, list):
        for e in v:
          py_typecheck.check_type(e, executor_base.Executor)
      for pl in [None, placement_literals.SERVER]:
        if pl in executor_placements:
          ex = executor_placements[pl]
          if isinstance(ex, list):
            pl_cardinality = len(ex)
            if pl_cardinality != 1:
              raise ValueError(
                  'Unsupported cardinality for placement "{}": {}.'.format(
                      pl, pl_cardinality))
  
  async def federated_secure_sum(self, arg):
    py_typecheck.check_type(arg.type_signature, computation_types.FederatedType)

    zero, plus = tuple(await asyncio.gather(*[
        executor_utils.embed_tf_scalar_constant(
            self.executor,
            arg.type_signature.member,
            0),
        executor_utils.embed_tf_binary_operator(
            self.executor,
            arg.type_signature.member,
            paillier.add)
    ]))

    ## TODO trusted aggr style reduce ##

    val = arg.internal_representation
    item_type = arg.type_signature.member
    py_typecheck.check_type(val, list)
    paillier_executor = self._get_child_executors(
        paillier_placement.PAILLIER, index=0)
    
    async def _move(v, from_, to_):
      to_executor = self._get_child_executors(paillier_placement.PAILLIER)
      channel = self.channel_grid[(from_, to_)]
      msg = await channel.send_hook(v)

      return await paillier_executor.create_value(await msg.compute(), item_type)

    items = await asyncio.gather(*[
        _move(v, placement_literal.CLIENTS, paillier_placement.PAILLIER)
        for v in val
    ])

    




