# Lint as: python3
# Copyright 2019, The TensorFlow Federated Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""An executor that handles federated types and federated operators."""

import abc
import asyncio

import absl.logging as logging
import tensorflow as tf

from tensorflow_federated.proto.v0 import computation_pb2 as pb
from tensorflow_federated.python.common_libs import anonymous_tuple
from tensorflow_federated.python.common_libs import py_typecheck
from tensorflow_federated.python.common_libs import tracing
from tensorflow_federated.python.core.api import computation_types
from tensorflow_federated.python.core.api import computations
from tensorflow_federated.python.core.impl import computation_impl
from tensorflow_federated.python.core.impl import type_utils
from tensorflow_federated.python.core.impl.compiler import intrinsic_defs
from tensorflow_federated.python.core.impl.compiler import placement_literals
from tensorflow_federated.python.core.impl.compiler import type_factory
from tensorflow_federated.python.core.impl.compiler import type_serialization
from tensorflow_federated.python.core.impl.executors import executor_base
from tensorflow_federated.python.core.impl.executors import executor_utils
from tensorflow_federated.python.core.impl.executors import executor_value_base

from tf_encrypted.primitives.sodium import easy_box


class FederatingExecutorValue(executor_value_base.ExecutorValue):
  """Represents a value embedded in the federated executor."""

  def __init__(self, value, type_spec):
    """Creates an embedded instance of a value in this executor.

    The kinds of supported internal representations (`value`) and types are as
    follows:

    * An instance of `intrinsic_defs.IntrinsicDef` in case of a federated
      operator (to be interpreted by this executor upon invocation).

    * An instance of `placement_literals.PlacementLiteral`.

    * An instance of `pb.Computation` in an unparsed form (to be relayed to one
      of the executors responsible for the given placement later on), which
      must be of one of the following varieties: tensorflow, lambda.

    * An ordinary Python `list` with values embedded in subordinate executors
      in case `type_spec` is a federated type. The list representation is used
      even if the value is of an `all_equal` type or there's only a single
      participant associated with the given placement.

    * A single value embedded in a subordinate executor in case `type_spec` is
      of a non-federated non-functional type.

    * An instance of `anonymous_tuple.AnonymousTuple` with values being one of
      the supported types listed above.

    This constructor does not perform any verification, however.

    Args:
      value: An internal value representation (of one of the allowed types, as
        defined above).
      type_spec: An instance of `tff.Type` or something convertible to it that
        is compatible with `value` (as defined above).
    """
    self._value = value
    self._type_signature = computation_types.to_type(type_spec)
    py_typecheck.check_type(type_spec, computation_types.Type)

  @property
  def internal_representation(self):
    return self._value

  @property
  def type_signature(self):
    return self._type_signature

  @tracing.trace
  async def compute(self):
    if isinstance(self._value, executor_value_base.ExecutorValue):
      return await self._value.compute()
    elif isinstance(self._type_signature, computation_types.FederatedType):
      py_typecheck.check_type(self._value, list)
      if self._type_signature.all_equal:
        if not self._value:
          # TODO(b/145936344): this happens when the executor has inferred the
          # cardinality of clients as 0, which can happen in tff.Computation
          # that only do a tff.federated_broadcast. This probably should be
          # handled elsewhere.
          raise RuntimeError('Arrived at a computation that inferred there are '
                             '0 clients. Try explicity passing `num_clients` '
                             'parameter when constructor the executor.')
        vals = [self._value[0]]
      else:
        vals = self._value
      results = []
      for v in vals:
        py_typecheck.check_type(v, executor_value_base.ExecutorValue)
        results.append(v.compute())
      results = await asyncio.gather(*results)
      if self._type_signature.all_equal:
        return results[0]
      else:
        return results
    elif isinstance(self._value, anonymous_tuple.AnonymousTuple):
      gathered_values = await asyncio.gather(*[
          FederatingExecutorValue(v, t).compute()
          for v, t in zip(self._value, self._type_signature)
      ])
      type_elements_iter = anonymous_tuple.iter_elements(self._type_signature)
      return anonymous_tuple.AnonymousTuple(
          (k, v) for (k, _), v in zip(type_elements_iter, gathered_values))
    else:
      raise RuntimeError(
          'Computing values of type {} represented as {} is not supported in '
          'this executor.'.format(self._type_signature,
                                  py_typecheck.type_string(type(self._value))))


class IntrinsicStrategy(abc.ABC):

  def __init__(self, federating_executor):
    self.federating_executor = federating_executor

  @classmethod
  @abc.abstractmethod
  def validate_executor_placements(cls, executor_placements):
    pass

  def _get_child_executors(self, placement, index=None):
    child_executors = self.federating_executor._target_executors[placement]
    if index is not None:
      return child_executors[index]
    return child_executors

  @classmethod
  def _check_arg_is_anonymous_tuple(cls, arg):
    py_typecheck.check_type(arg.type_signature,
                            computation_types.NamedTupleType)
    py_typecheck.check_type(arg.internal_representation,
                            anonymous_tuple.AnonymousTuple)

  @tracing.trace
  async def _place(self, arg, placement):
    py_typecheck.check_type(placement, placement_literals.PlacementLiteral)
    children = self._get_child_executors(placement)
    val = await arg.compute()
    return FederatingExecutorValue(
        await asyncio.gather(
            *[c.create_value(val, arg.type_signature) for c in children]),
        computation_types.FederatedType(
            arg.type_signature, placement, all_equal=True))

  @tracing.trace
  async def _eval(self, arg, placement, all_equal):
    py_typecheck.check_type(arg.type_signature, computation_types.FunctionType)
    py_typecheck.check_none(arg.type_signature.parameter)
    py_typecheck.check_type(arg.internal_representation, pb.Computation)
    py_typecheck.check_type(placement, placement_literals.PlacementLiteral)
    fn = arg.internal_representation
    fn_type = arg.type_signature
    children = self._get_child_executors(placement)

    async def call(child):
      return await child.create_call(await child.create_value(fn, fn_type))

    results = await asyncio.gather(*[call(child) for child in children])
    return FederatingExecutorValue(
        results,
        computation_types.FederatedType(
            fn_type.result, placement, all_equal=all_equal))

  @tracing.trace
  async def _map(self, arg, all_equal=None):
    self._check_arg_is_anonymous_tuple(arg)
    py_typecheck.check_len(arg.internal_representation, 2)
    fn_type = arg.type_signature[0]
    py_typecheck.check_type(fn_type, computation_types.FunctionType)
    val_type = arg.type_signature[1]
    py_typecheck.check_type(val_type, computation_types.FederatedType)
    if all_equal is None:
      all_equal = val_type.all_equal
    elif all_equal and not val_type.all_equal:
      raise ValueError(
          'Cannot map a non-all_equal argument into an all_equal result.')
    fn = arg.internal_representation[0]
    py_typecheck.check_type(fn, pb.Computation)
    val = arg.internal_representation[1]
    py_typecheck.check_type(val, list)
    for v in val:
      py_typecheck.check_type(v, executor_value_base.ExecutorValue)
    children = self._get_child_executors(val_type.placement)
    fns = await asyncio.gather(*[c.create_value(fn, fn_type) for c in children])
    results = await asyncio.gather(*[
        c.create_call(f, v) for c, (f, v) in zip(children, list(zip(fns, val)))
    ])
    return FederatingExecutorValue(
        results,
        computation_types.FederatedType(
            fn_type.result, val_type.placement, all_equal=all_equal))

  @tracing.trace
  async def _zip(self, arg, placement, all_equal):
    self._check_arg_is_anonymous_tuple(arg)
    py_typecheck.check_type(placement, placement_literals.PlacementLiteral)
    children = self._get_child_executors(placement)
    cardinality = len(children)
    elements = anonymous_tuple.to_elements(arg.internal_representation)
    for _, v in elements:
      py_typecheck.check_type(v, list)
      if len(v) != cardinality:
        raise RuntimeError('Expected {} items, found {}.'.format(
            cardinality, len(v)))
    new_vals = []
    for idx in range(cardinality):
      new_vals.append(
          anonymous_tuple.AnonymousTuple([(k, v[idx]) for k, v in elements]))
    new_vals = await asyncio.gather(
        *[c.create_tuple(x) for c, x in zip(children, new_vals)])
    return FederatingExecutorValue(
        new_vals,
        computation_types.FederatedType(
            computation_types.NamedTupleType((
                (k, v.member) if k else v.member
                for k, v in anonymous_tuple.iter_elements(arg.type_signature))),
            placement,
            all_equal=all_equal))

  @classmethod
  async def _move(cls, value, value_type, target_executor):
    return await target_executor.create_value(await value.compute(), value_type)


class CentralizedIntrinsicStrategy(IntrinsicStrategy):

  def __init__(self, federating_executor):
    super().__init__(federating_executor)

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

  async def federated_value_at_server(self, arg):
    return await self._place(arg, placement_literals.SERVER)

  async def federated_value_at_clients(self, arg):
    return await self._place(arg, placement_literals.CLIENTS)

  async def federated_eval_at_server(self, arg):
    return await self._eval(arg, placement_literals.SERVER, True)

  async def federated_eval_at_clients(self, arg):
    return await self._eval(arg, placement_literals.CLIENTS, False)

  async def federated_apply(self, arg):
    return await self._map(arg)

  async def federated_map(self, arg):
    return await self._map(arg, all_equal=False)

  async def federated_map_all_equal(self, arg):
    return await self._map(arg, all_equal=True)

  async def federated_broadcast(self, arg):
    py_typecheck.check_type(arg.type_signature, computation_types.FederatedType)
    py_typecheck.check_type(arg.internal_representation, list)
    if not arg.type_signature.all_equal:
      raise ValueError('Cannot broadcast a non all_equal value.')
    if len(arg.internal_representation) != 1:
      raise ValueError(
          'Cannot broadcast a with a non-singleton representation.')
    val = await arg.internal_representation[0].compute()
    return FederatingExecutorValue(
        await asyncio.gather(*[
            c.create_value(val, arg.type_signature.member)
            for c in self._get_child_executors(placement_literals.CLIENTS)
        ]), type_factory.at_clients(arg.type_signature.member, all_equal=True))

  async def federated_zip_at_server(self, arg):
    return await self._zip(arg, placement_literals.SERVER, all_equal=True)

  async def federated_zip_at_clients(self, arg):
    return await self._zip(arg, placement_literals.CLIENTS, all_equal=False)

  async def federated_reduce(self, arg):
    self._check_arg_is_anonymous_tuple(arg)
    if len(arg.internal_representation) != 3:
      raise ValueError(
          'Expected 3 elements in the `federated_reduce()` argument tuple, '
          'found {}.'.format(len(arg.internal_representation)))

    val_type = arg.type_signature[0]
    py_typecheck.check_type(val_type, computation_types.FederatedType)
    item_type = val_type.member
    zero_type = arg.type_signature[1]
    op_type = arg.type_signature[2]
    type_utils.check_equivalent_types(
        op_type, type_factory.reduction_op(zero_type, item_type))

    val = arg.internal_representation[0]
    py_typecheck.check_type(val, list)
    child = self._get_child_executors(placement_literals.SERVER, index=0)

    items = await asyncio.gather(
        *[self._move(v, item_type, child) for v in val])

    zero = await child.create_value(
        await (await self.federating_executor.create_selection(
            arg, index=1)).compute(), zero_type)
    op = await child.create_value(arg.internal_representation[2], op_type)

    result = zero
    for item in items:
      result = await child.create_call(
          op, await child.create_tuple(
              anonymous_tuple.AnonymousTuple([(None, result), (None, item)])))
    return FederatingExecutorValue([result],
                                   computation_types.FederatedType(
                                       result.type_signature,
                                       placement_literals.SERVER,
                                       all_equal=True))

  async def federated_aggregate(self, arg):
    val_type, zero_type, accumulate_type, _, report_type = (
        executor_utils.parse_federated_aggregate_argument_types(
            arg.type_signature))
    py_typecheck.check_type(arg.internal_representation,
                            anonymous_tuple.AnonymousTuple)
    py_typecheck.check_len(arg.internal_representation, 5)

    # Note: This is a simple initial implementation that simply forwards this
    # to `federated_reduce()`. The more complete implementation would be able
    # to take advantage of the parallelism afforded by `merge` to reduce the
    # cost from liner (with respect to the number of clients) to sub-linear.

    # TODO(b/134543154): Expand this implementation to take advantage of the
    # parallelism afforded by `merge`.
    fed_ex = self.federating_executor

    val = arg.internal_representation[0]
    zero = arg.internal_representation[1]
    accumulate = arg.internal_representation[2]
    pre_report = await fed_ex._compute_intrinsic_federated_reduce(
        FederatingExecutorValue(
            anonymous_tuple.AnonymousTuple([(None, val), (None, zero),
                                            (None, accumulate)]),
            computation_types.NamedTupleType(
                (val_type, zero_type, accumulate_type))))

    py_typecheck.check_type(pre_report.type_signature,
                            computation_types.FederatedType)
    type_utils.check_equivalent_types(pre_report.type_signature.member,
                                      report_type.parameter)

    report = arg.internal_representation[4]
    return await fed_ex._compute_intrinsic_federated_apply(
        FederatingExecutorValue(
            anonymous_tuple.AnonymousTuple([
                (None, report), (None, pre_report.internal_representation)
            ]),
            computation_types.NamedTupleType(
                (report_type, pre_report.type_signature))))

  async def federated_sum(self, arg):
    py_typecheck.check_type(arg.type_signature, computation_types.FederatedType)
    zero, plus = tuple(await asyncio.gather(*[
        executor_utils.embed_tf_scalar_constant(
            self.federating_executor,
            arg.type_signature.member,
            0),
        executor_utils.embed_tf_binary_operator(
            self.federating_executor,
            arg.type_signature.member,
            tf.add)]))
    return await self.federating_executor._compute_intrinsic_federated_reduce(
        FederatingExecutorValue(
            anonymous_tuple.AnonymousTuple([
                (None, arg.internal_representation),
                (None, zero.internal_representation),
                (None, plus.internal_representation)
            ]),
            computation_types.NamedTupleType(
                (arg.type_signature, zero.type_signature, plus.type_signature)))
    )

  async def federated_mean(self, arg):
    arg_sum = await self.federating_executor._compute_intrinsic_federated_sum(arg)
    member_type = arg_sum.type_signature.member
    count = float(len(arg.internal_representation))
    if count < 1.0:
      raise RuntimeError('Cannot compute a federated mean over an empty group.')
    child = self._get_child_executors(placement_literals.SERVER, index=0)
    factor, multiply = tuple(await asyncio.gather(*[
        executor_utils.embed_tf_scalar_constant(child, member_type,
                                                float(1.0 / count)),
        executor_utils.embed_tf_binary_operator(child, member_type, tf.multiply)
    ]))
    multiply_arg = await child.create_tuple(
        anonymous_tuple.AnonymousTuple([(None,
                                         arg_sum.internal_representation[0]),
                                        (None, factor)]))
    result = await child.create_call(multiply, multiply_arg)
    return FederatingExecutorValue([result], arg_sum.type_signature)

  async def federated_weighted_mean(self, arg):
    return await executor_utils.compute_federated_weighted_mean(
        self.federating_executor, arg)

  async def federated_collect(self, arg):
    py_typecheck.check_type(arg.type_signature, computation_types.FederatedType)
    type_utils.check_federated_type(
        arg.type_signature, placement=placement_literals.CLIENTS)
    val = arg.internal_representation
    py_typecheck.check_type(val, list)
    member_type = arg.type_signature.member
    child = self._get_child_executors(placement_literals.SERVER, index=0)
    collected_items = await child.create_value(
        await asyncio.gather(*[v.compute() for v in val]),
        computation_types.SequenceType(member_type))
    return FederatingExecutorValue(
        [collected_items],
        computation_types.FederatedType(
            computation_types.SequenceType(member_type),
            placement_literals.SERVER))

  async def federated_secure_sum(self, arg):
    raise NotImplementedError('The secure sum intrinsic is not implemented.')


class TrustedAggregatorIntrinsicStrategy(IntrinsicStrategy):

  def __init__(self, federating_executor):
    super().__init__(federating_executor)

  @classmethod
  def validate_executor_placements(cls, executor_placements):
    singleton_placements = [
        None,
        placement_literals.SERVER,
        placement_literals.AGGREGATORS,
    ]
    expected_placements = singleton_placements + [placement_literals.CLIENTS]
    py_typecheck.check_type(executor_placements, dict)
    for k, v in executor_placements.items():
      if k is not None:
        py_typecheck.check_type(k, placement_literals.PlacementLiteral)
      py_typecheck.check_type(v, (list, executor_base.Executor))
      if isinstance(v, list):
        for e in v:
          py_typecheck.check_type(e, executor_base.Executor)
    for pl in expected_placements:
      if pl not in executor_placements:
        raise ValueError(
            'Improper executor placements, expected executor stack for '
            '{} placement.'.format(pl.name))
    for pl in singleton_placements:
      if pl in executor_placements:
        ex = executor_placements[pl]
        if isinstance(ex, list):
          pl_cardinality = len(ex)
          if pl_cardinality != 1:
            raise ValueError(
                'Unsupported cardinality for placement "{}": {}.'.format(
                    pl, pl_cardinality))

  async def _trusted_aggregator_generate_keys(self):
  
    pk, sk = easy_box.gen_keypair()

    # Place public key on clients
    clients_ex = self._get_child_executors(placement_literals.CLIENTS)
    pk_eager_vals = []
    pk_type = computation_types.TensorType(tf.uint8, pk.raw.shape)
    for i in range(len(clients_ex)):
      pk_eager_val = await clients_ex[i].create_value(pk.raw, pk_type)
      pk_eager_vals.append(pk_eager_val)

    pk_a_type = computation_types.FederatedType(
        pk_type,
        placement_literals.CLIENTS,
        all_equal=True)

    pk_fed_val = FederatingExecutorValue(pk_eager_vals, pk_type)

    # Place secret key on the trusted aggregator
    aggregator_ex = self._get_child_executors(placement_literals.AGGREGATOR)
    sk_eager_val = await aggregator_ex[0].create_value(sk.raw, pk_type)
    sk_fed_val = FederatingExecutorValue(sk_eager_val, pk_type)

    return pk_fed_val, sk_fed_val

  async def _zip_val_key(self, val, key):
    
    # zip values and keys EagerValues on each client using their 
    # respective eager executor
    val_key_zipped = []
    for i in range(len(val)):
      client_ex = self._get_child_executors(placement_literals.CLIENTS, index=i)
      val_eager_val = val[i]
      if isinstance(key.internal_representation, list):
        key_eager_val = key.internal_representation[i]
      else:
        key_eager_val = key.internal_representation

      k_v_zip = await client_ex.create_tuple(
          anonymous_tuple.AnonymousTuple([(None, val_eager_val), 
                                          (None, key_eager_val)])
      )

      val_key_zipped.append(k_v_zip)

    return val_key_zipped

  async def _encrypt_client_tensors(self, arg, pk_a):

    input_tensor_type = arg.type_signature[0].member
    pk_a_tensor_type = pk_a.type_signature

    @computations.tf_computation(input_tensor_type, 
                                 pk_a_tensor_type)
    def encrypt_tensor(plaintext, pk_a):

      pk_a = easy_box.PublicKey(pk_a)
      pk_c, sk_c = easy_box.gen_keypair()
      nonce = easy_box.gen_nonce()
      ciphertext, mac = easy_box.seal_detached(plaintext, 
                                               nonce, 
                                               pk_a, 
                                               sk_c
        )

      return ciphertext.raw, mac.raw, pk_c.raw, nonce.raw

    fn_type = encrypt_tensor.type_signature
    fn = encrypt_tensor._computation_proto
    val_type = arg.type_signature[0]
    val = arg.internal_representation[0]

    val_key_zipped = await self._zip_val_key(val, pk_a)

    fed_ex = self.federating_executor

    return await fed_ex._compute_intrinsic_federated_map(
        FederatingExecutorValue(
            anonymous_tuple.AnonymousTuple(
                [(None, fn), (None,  val_key_zipped)]),
            computation_types.NamedTupleType(
                (fn_type, val_type))))

  async def _decrypt_tensors_on_aggregator(self, val, clients_dtype):

    clients_output_type_signature = val[0].type_signature[0]
    ciphtertext_tensor_type = clients_output_type_signature[0]
    mac_tensor_type = clients_output_type_signature[1]
    pk_c_tensor_type = clients_output_type_signature[2]
    nonce_tensor_type = clients_output_type_signature[3]
    sk_a_tensor_type = clients_output_type_signature[3]

    @computations.tf_computation(ciphtertext_tensor_type, 
                                 mac_tensor_type, 
                                 pk_c_tensor_type,
                                 nonce_tensor_type, 
                                 sk_a_tensor_type)
    def decrypt_tensor(ciphertext, mac, pk_c, nonce, sk_a):
      
      ciphertext = easy_box.Ciphertext(ciphertext)
      mac = easy_box.Mac(mac)
      pk_c = easy_box.PublicKey(pk_c)
      nonce = easy_box.Nonce(nonce)
      sk_a = easy_box.SecretKey(sk_a)

      plaintext_recovered = easy_box.open_detached(ciphertext, 
                                                   mac, 
                                                   nonce, 
                                                   pk_c, 
                                                   sk_a, 
                                                   clients_dtype)

      return plaintext_recovered

    val_type = computation_types.FederatedType(
        computation_types.TensorType(clients_dtype),
        placement_literals.SERVER,
        all_equal=False)

    fn_type = decrypt_tensor.type_signature
    fn = decrypt_tensor._computation_proto

    fed_ex = self.federating_executor

    return await fed_ex._compute_intrinsic_federated_map(
        FederatingExecutorValue(
            anonymous_tuple.AnonymousTuple(
                [(None, fn), (None,  val)]),
            computation_types.NamedTupleType(
                (fn_type, val_type))))

  async def federated_value_at_server(self, arg):
    return await self._place(arg, placement_literals.SERVER)

  async def federated_value_at_clients(self, arg):
    return await self._place(arg, placement_literals.CLIENTS)

  async def federated_eval_at_server(self, arg):
    return await self._eval(arg, placement_literals.SERVER, True)

  async def federated_eval_at_clients(self, arg):
    return await self._eval(arg, placement_literals.CLIENTS, False)

  async def federated_apply(self, arg):
    return await self._map(arg)

  async def federated_map(self, arg):
    return await self._map(arg, all_equal=False)

  async def federated_map_all_equal(self, arg):
    return await self._map(arg, all_equal=True)

  async def federated_broadcast(self, arg):
    py_typecheck.check_type(arg.type_signature, computation_types.FederatedType)
    py_typecheck.check_type(arg.internal_representation, list)
    if not arg.type_signature.all_equal:
      raise ValueError('Cannot broadcast a non all_equal value.')
    if len(arg.internal_representation) != 1:
      raise ValueError(
          'Cannot broadcast a with a non-singleton representation.')
    val = await arg.internal_representation[0].compute()
    return FederatingExecutorValue(
        await asyncio.gather(*[
            c.create_value(val, arg.type_signature.member)
            for c in self._get_child_executors(placement_literals.CLIENTS)
        ]), type_factory.at_clients(arg.type_signature.member, all_equal=True))

  async def federated_zip_at_server(self, arg):
    return await self._zip(arg, placement_literals.SERVER, all_equal=True)

  async def federated_zip_at_clients(self, arg):
    return await self._zip(arg, placement_literals.CLIENTS, all_equal=False)

  async def federated_reduce(self, arg):
    self._check_arg_is_anonymous_tuple(arg)
    if len(arg.internal_representation) != 3:
      raise ValueError(
          'Expected 3 elements in the `federated_reduce()` argument tuple, '
          'found {}.'.format(len(arg.internal_representation)))

    #Encrypt client tensors
    pk_a, sk_a = await self._trusted_aggregator_generate_keys()
    enc_client_arg = await self._encrypt_client_tensors(arg, pk_a)
    val_type = enc_client_arg.type_signature
    val = enc_client_arg.internal_representation
    # Store original client dtype before encryption for 
    # future decryption
    orig_clients_dtype = arg.type_signature[0].member.dtype

    py_typecheck.check_type(val_type, computation_types.FederatedType)
    item_type = val_type.member
    zero_type = arg.type_signature[1]
    op_type = arg.type_signature[2]

    # Note: this type check is not working because of the new types
    # returned by encrypt tensors: Types (<float32,float32> -> float32) and 
    # (<float32,<uint8[4],uint8[16],uint8[32],uint8[24]>> -> float32) are not equivalent
    # type_utils.check_equivalent_types(
    #     op_type, type_factory.reduction_op(zero_type, item_type))

    py_typecheck.check_type(val, list)
    aggr = self._get_child_executors(placement_literals.AGGREGATORS, index=0)

    aggregands = await asyncio.gather(
        *[self._move(v, item_type, aggr) for v in val])

    aggregands_key_zipped = await self._zip_val_key(aggregands, sk_a)
    
    # Decrypt tensors moved to the server before applying reduce
    # In the future should be decrypted by the Trusted aggregator
    aggregands_decrypted = []
    for item in aggregands_key_zipped:
      decrypted_tensor = await self._decrypt_tensors_on_aggregator(
          [item], 
          orig_clients_dtype
        )
      aggregands_decrypted.append(decrypted_tensor.internal_representation[0])

    zero = await aggr.create_value(
        await (await self.federating_executor.create_selection(
            arg, index=1)).compute(),
        zero_type)
    op = await aggr.create_value(arg.internal_representation[2], op_type)

    result = zero
    for item in aggregands_decrypted:
      result = await aggr.create_call(
          op, await aggr.create_tuple(
              anonymous_tuple.AnonymousTuple([(None, result), (None, item)])))
    return FederatingExecutorValue([result],
                                   computation_types.FederatedType(
                                       result.type_signature,
                                       placement_literals.SERVER,
                                       all_equal=True))

  async def federated_aggregate(self, arg):
    val_type, zero_type, accumulate_type, _, report_type = (
        executor_utils.parse_federated_aggregate_argument_types(
            arg.type_signature))
    py_typecheck.check_type(arg.internal_representation,
                            anonymous_tuple.AnonymousTuple)
    py_typecheck.check_len(arg.internal_representation, 5)

    # Note: This is a simple initial implementation that simply forwards this
    # to `federated_reduce()`. The more complete implementation would be able
    # to take advantage of the parallelism afforded by `merge` to reduce the
    # cost from liner (with respect to the number of clients) to sub-linear.

    # TODO(b/134543154): Expand this implementation to take advantage of the
    # parallelism afforded by `merge`.
    fed_ex = self.federating_executor

    val = arg.internal_representation[0]
    zero = arg.internal_representation[1]
    accumulate = arg.internal_representation[2]
    pre_report = await fed_ex._compute_intrinsic_federated_reduce(
        FederatingExecutorValue(
            anonymous_tuple.AnonymousTuple([(None, val), (None, zero),
                                            (None, accumulate)]),
            computation_types.NamedTupleType(
                (val_type, zero_type, accumulate_type))))

    py_typecheck.check_type(pre_report.type_signature,
                            computation_types.FederatedType)
    type_utils.check_equivalent_types(pre_report.type_signature.member,
                                      report_type.parameter)

    report = arg.internal_representation[4]
    return await fed_ex._compute_intrinsic_federated_apply(
        FederatingExecutorValue(
            anonymous_tuple.AnonymousTuple([
                (None, report), (None, pre_report.internal_representation)
            ]),
            computation_types.NamedTupleType(
                (report_type, pre_report.type_signature))))

  async def federated_sum(self, arg):
    py_typecheck.check_type(arg.type_signature, computation_types.FederatedType)
    zero, plus = tuple(await asyncio.gather(*[
        executor_utils.embed_tf_scalar_constant(
            self.federating_executor,
            arg.type_signature.member,
            0),
        executor_utils.embed_tf_binary_operator(
            self.federating_executor,
            arg.type_signature.member,
            tf.add)]))
    return await self.federating_executor._compute_intrinsic_federated_reduce(
        FederatingExecutorValue(
            anonymous_tuple.AnonymousTuple([
                (None, arg.internal_representation),
                (None, zero.internal_representation),
                (None, plus.internal_representation)
            ]),
            computation_types.NamedTupleType(
                (arg.type_signature, zero.type_signature, plus.type_signature)))
    )

  async def federated_mean(self, arg):
    arg_sum = await self.federating_executor._compute_intrinsic_federated_sum(arg)
    member_type = arg_sum.type_signature.member
    count = float(len(arg.internal_representation))
    if count < 1.0:
      raise RuntimeError('Cannot compute a federated mean over an empty group.')
    child = self._get_child_executors(placement_literals.AGGREGATORS, index=0)
    factor, multiply = tuple(await asyncio.gather(*[
    ]))
    multiply_arg = await child.create_tuple(
        anonymous_tuple.AnonymousTuple([(None,
                                         arg_sum.internal_representation[0]),
                                        (None, factor)]))
    result = await child.create_call(multiply, multiply_arg)
    return FederatingExecutorValue([result], arg_sum.type_signature)

  async def federated_weighted_mean(self, arg):
    return await executor_utils.compute_federated_weighted_mean(
        self.federating_executor, arg)

  async def federated_collect(self, arg):
    raise NotImplementedError(
        'The collect intrinsic is not implemented for {}.'.format(
            self.__class__.__name__))

  async def federated_secure_sum(self, arg):
    raise NotImplementedError('The secure sum intrinsic is not implemented.')


class FederatingExecutor(executor_base.Executor):
  """The federated executor orchestrates federated computations.

  The intrinsics currently implemented include:

  * federated_aggregate
  * federated_apply
  * federated_broadcast
  * federated_collect
  * federated_eval
  * federated_map
  * federated_mean
  * federated_reduce
  * federated_sum
  * federated_value
  * federated_weighted_mean
  * federated_zip

  This executor is only responsible for handling federated types and federated
  operators, and a delegation of work to an underlying collection of target
  executors associated with individual system participants. This executor does
  not interpret lambda calculus and compositional constructs (blocks, etc.).
  It understands placements, selected intrinsics (federated operators), it can
  handle tuples, selections, and calls in a limited way (to the extent that it
  deals with intrinsics or lambda expressions it can delegate).

  The initial implementation of the executor only supports the two basic types
  of placements (SERVER and CLIENTS), and does not have a built-in concept of
  intermediate aggregation, partitioning placements, clustering clients, etc.

  The initial implementation also does not attempt at performing optimizations
  in case when the constituents of this executor are either located on the same
  machine (where marshaling/unmarshaling could be avoided), or when they have
  the `all_equal` property (and a single value could be shared by them all).
  """

  # TODO(b/134543154): Extend this executor to support intermediate aggregation
  # and other optimizations hinted above.

  # TODO(b/134543154): Add support for `data` as a building block.

  # TODO(b/134543154): Implement the commonly used aggregation intrinsics so we
  # can begin to use this executor in integration tests.

  def __init__(self, target_executors, intrinsic_strategy_fn=CentralizedIntrinsicStrategy):
    """Creates a federated executor backed by a collection of target executors.

    Args:
      target_executors: A dictionary mapping placements to executors or lists of
        executors associated with these placements. The keys in this dictionary
        can be either placement literals, or `None` to specify the executor for
        unplaced computations. The values can be either single executors (if
        there only is a single participant associated with that placement, as
        would typically be the case with `tff.SERVER`) or lists of target
        executors.
      intrinsic_strategy_fn: A callable mapping the current executor instance to
        an instantiation of an IntrinsicStrategy implementation.

    Raises:
      ValueError: If the target_executors are improper for the given
        intrinsic_strategy_fn.
    """
    py_typecheck.check_callable(intrinsic_strategy_fn)
    intrinsic_strategy = intrinsic_strategy_fn(self)
    py_typecheck.check_type(intrinsic_strategy, IntrinsicStrategy)
    intrinsic_strategy.validate_executor_placements(target_executors)
    self.intrinsic_strategy = intrinsic_strategy

    self._target_executors = {}
    for k, v in target_executors.items():
      # v is either an Executor or a list of Executors
      if isinstance(v, executor_base.Executor):
        self._target_executors[k] = [v]
      else:
        self._target_executors[k] = v.copy()

  def close(self):
    for p, v in self._target_executors.items():
      for e in v:
        logging.debug('Closing child executor for placement: %s', p)
        e.close()

  @tracing.trace(stats=False)
  async def create_value(self, value, type_spec=None):
    """A coroutine that creates embedded value from `value` of type `type_spec`.

    See the `FederatingExecutorValue` for detailed information about the
    `value`s and `type_spec`s that can be embedded using `create_value`.

    Args:
      value: An object that represents the value to embed within the executor.
      type_spec: An optional `tff.Type` of the value represented by this object,
        or something convertible to it.

    Returns:
      An instance of `FederatingExecutorValue` that represents the embedded
      value.

    Raises:
      TypeError: If the `value` and `type_spec` do not match.
      ValueError: If `value` is not a kind recognized by the
        `FederatingExecutor`.
    """
    type_spec = computation_types.to_type(type_spec)
    if isinstance(value, intrinsic_defs.IntrinsicDef):
      if not type_utils.is_concrete_instance_of(type_spec,
                                                value.type_signature):
        raise TypeError('Incompatible type {} used with intrinsic {}.'.format(
            type_spec, value.uri))
      return FederatingExecutorValue(value, type_spec)
    elif isinstance(value, placement_literals.PlacementLiteral):
      if type_spec is None:
        type_spec = computation_types.PlacementType()
      else:
        py_typecheck.check_type(type_spec, computation_types.PlacementType)
      return FederatingExecutorValue(value, type_spec)
    elif isinstance(value, computation_impl.ComputationImpl):
      return await self.create_value(
          computation_impl.ComputationImpl.get_proto(value),
          type_utils.reconcile_value_with_type_spec(value, type_spec))
    elif isinstance(value, pb.Computation):
      if type_spec is None:
        type_spec = type_serialization.deserialize_type(value.type)
      which_computation = value.WhichOneof('computation')
      if which_computation in ['lambda', 'tensorflow']:
        return FederatingExecutorValue(value, type_spec)
      elif which_computation == 'reference':
        raise ValueError(
            'Encountered an unexpected unbound references "{}".'.format(
                value.reference.name))
      elif which_computation == 'intrinsic':
        intr = intrinsic_defs.uri_to_intrinsic_def(value.intrinsic.uri)
        if intr is None:
          raise ValueError('Encountered an unrecognized intrinsic "{}".'.format(
              value.intrinsic.uri))
        py_typecheck.check_type(intr, intrinsic_defs.IntrinsicDef)
        return await self.create_value(intr, type_spec)
      elif which_computation == 'placement':
        return await self.create_value(
            placement_literals.uri_to_placement_literal(value.placement.uri),
            type_spec)
      elif which_computation == 'call':
        parts = [value.call.function]
        if value.call.argument.WhichOneof('computation'):
          parts.append(value.call.argument)
        parts = await asyncio.gather(*[self.create_value(x) for x in parts])
        return await self.create_call(parts[0],
                                      parts[1] if len(parts) > 1 else None)
      elif which_computation == 'tuple':
        element_values = await asyncio.gather(
            *[self.create_value(x.value) for x in value.tuple.element])
        return await self.create_tuple(
            anonymous_tuple.AnonymousTuple(
                (e.name if e.name else None, v)
                for e, v in zip(value.tuple.element, element_values)))
      elif which_computation == 'selection':
        which_selection = value.selection.WhichOneof('selection')
        if which_selection == 'name':
          name = value.selection.name
          index = None
        elif which_selection != 'index':
          raise ValueError(
              'Unrecognized selection type: "{}".'.format(which_selection))
        else:
          index = value.selection.index
          name = None
        return await self.create_selection(
            await self.create_value(value.selection.source),
            index=index,
            name=name)
      else:
        raise ValueError(
            'Unsupported computation building block of type "{}".'.format(
                which_computation))
    else:
      py_typecheck.check_type(type_spec, computation_types.Type)
      if isinstance(type_spec, computation_types.FunctionType):
        raise ValueError(
            'Encountered a value of a functional TFF type {} and Python type '
            '{} that is not of one of the recognized representations.'.format(
                type_spec, py_typecheck.type_string(type(value))))
      elif isinstance(type_spec, computation_types.FederatedType):
        children = self._target_executors.get(type_spec.placement)
        if not children:
          raise ValueError(
              'Placement "{}" is not configured in this executor.'.format(
                  type_spec.placement))
        py_typecheck.check_type(children, list)
        if not type_spec.all_equal:
          py_typecheck.check_type(value, (list, tuple, set, frozenset))
          if not isinstance(value, list):
            value = list(value)
        elif isinstance(value, list):
          raise ValueError(
              'An all_equal value should be passed directly, not as a list.')
        else:
          value = [value for _ in children]
        if len(value) != len(children):
          raise ValueError(
              'Federated value contains {} items, but the placement {} in this '
              'executor is configured with {} participants.'.format(
                  len(value), type_spec.placement, len(children)))
        child_vals = await asyncio.gather(*[
            c.create_value(v, type_spec.member)
            for v, c in zip(value, children)
        ])
        return FederatingExecutorValue(child_vals, type_spec)
      else:
        child = self._target_executors.get(None)
        if not child or len(child) > 1:
          raise ValueError('Executor is not configured for unplaced values.')
        else:
          return FederatingExecutorValue(
              await child[0].create_value(value, type_spec), type_spec)

  @tracing.trace
  async def create_call(self, comp, arg=None):
    """A coroutine that creates a call to `comp` with optional argument `arg`.

    Args:
      comp: The computation to invoke.
      arg: An optional argument of the call, or `None` if no argument was
        supplied.

    Returns:
      An instance of `FederatingExecutorValue` that represents the constructed
      call.

    Raises:
      TypeError: If the `type_signature` of `arg` does not match the expected
        `type_signature` of the parameter to `comp`.
      ValueError: If `comp` is not a functional kind recognized by the
        `FederatingExecutor` or if `comp` is a lambda with an argument.
      NotImplementedError: If `comp` is an intrinsic and it has not been
        implemented by the `FederatingExecutor`.
    """
    py_typecheck.check_type(comp, FederatingExecutorValue)
    if arg is not None:
      py_typecheck.check_type(arg, FederatingExecutorValue)
      py_typecheck.check_type(comp.type_signature,
                              computation_types.FunctionType)
      param_type = comp.type_signature.parameter
      type_utils.check_assignable_from(param_type, arg.type_signature)
      arg = FederatingExecutorValue(arg.internal_representation, param_type)
    if isinstance(comp.internal_representation, pb.Computation):
      which_computation = comp.internal_representation.WhichOneof('computation')
      if which_computation == 'lambda':
        if comp.type_signature.parameter is not None:
          raise ValueError(
              'Directly calling lambdas with arguments is unsupported. '
              'Found call to lambda with type {}.'.format(comp.type_signature))
        # Pull the inner computation out of called no-arg lambdas.
        return await self.create_value(
            getattr(comp.internal_representation, 'lambda').result,
            comp.type_signature.result)
      elif which_computation == 'tensorflow':
        # Run tensorflow computations.
        child = self._target_executors[None][0]
        embedded_comp = await child.create_value(comp.internal_representation,
                                                 comp.type_signature)
        if arg is not None:
          embedded_arg = await executor_utils.delegate_entirely_to_executor(
              arg.internal_representation, arg.type_signature, child)
        else:
          embedded_arg = None
        result = await child.create_call(embedded_comp, embedded_arg)
        return FederatingExecutorValue(result, result.type_signature)
      else:
        raise ValueError(
            'Directly calling computations of type {} is unsupported.'.format(
                which_computation))
    elif isinstance(comp.internal_representation, intrinsic_defs.IntrinsicDef):
      coro = getattr(
          self,
          '_compute_intrinsic_{}'.format(comp.internal_representation.uri),
          None)
      if coro is not None:
        return await coro(arg)  # pylint: disable=not-callable
      else:
        raise NotImplementedError(
            'Support for intrinsic "{}" has not been implemented yet.'.format(
                comp.internal_representation.uri))
    else:
      raise ValueError('Calling objects of type {} is unsupported.'.format(
          py_typecheck.type_string(type(comp.internal_representation))))

  @tracing.trace
  async def create_tuple(self, elements):
    """A coroutine that creates a tuple of `elements`.

    Args:
      elements: A collection of `ExecutorValue`s to create a tuple from.

    Returns:
      An instance of `FederatingExecutorValue` that represents the constructed
      tuple.
    """
    for value in elements:
      py_typecheck.check_type(value, FederatingExecutorValue)
    elements = anonymous_tuple.to_elements(
        anonymous_tuple.from_container(elements))
    return FederatingExecutorValue(
        anonymous_tuple.AnonymousTuple(
            (k, v.internal_representation) for k, v in elements),
        computation_types.NamedTupleType(
            (k, v.type_signature) if k else v.type_signature
            for k, v in elements))

  @tracing.trace
  async def create_selection(self, source, index=None, name=None):
    py_typecheck.check_type(source, FederatingExecutorValue)
    py_typecheck.check_type(source.type_signature,
                            computation_types.NamedTupleType)
    if name is not None:
      name_to_index = dict((n, i) for i, (
          n,
          t) in enumerate(anonymous_tuple.to_elements(source.type_signature)))
      index = name_to_index[name]
    if isinstance(source.internal_representation,
                  anonymous_tuple.AnonymousTuple):
      val = source.internal_representation
      selected = val[index]
      return FederatingExecutorValue(selected, source.type_signature[index])
    elif isinstance(source.internal_representation,
                    executor_value_base.ExecutorValue):
      if type_utils.type_tree_contains_types(source.type_signature,
                                             computation_types.FederatedType):
        raise ValueError(
            'FederatingExecutorValue {} has violated its contract; '
            'it is embedded in another executor and yet its type '
            'has placement. The embedded value is {}, with type '
            'signature {}.'.format(source, source.internal_representation,
                                   source.type_signature))
      val = source.internal_representation
      child = self._target_executors[None][0]
      return FederatingExecutorValue(
          await child.create_selection(val, index=index),
          source.type_signature[index])
    else:
      raise ValueError('Unexpected internal representation while creating '
                       'selection. Expected one of `AnonymousTuple` or value '
                       'embedded in target executor, received {}'.format(
                           source.internal_representation))

  @tracing.trace
  async def _compute_intrinsic_federated_value_at_server(self, arg):
    return await self.intrinsic_strategy.federated_value_at_server(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_value_at_clients(self, arg):
    return await self.intrinsic_strategy.federated_value_at_clients(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_eval_at_server(self, arg):
    return await self.intrinsic_strategy.federated_eval_at_server(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_eval_at_clients(self, arg):
    return await self.intrinsic_strategy.federated_eval_at_clients(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_apply(self, arg):
    return await self.intrinsic_strategy.federated_apply(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_map(self, arg):
    return await self.intrinsic_strategy.federated_map(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_map_all_equal(self, arg):
    return await self.intrinsic_strategy.federated_map_all_equal(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_broadcast(self, arg):
    return await self.intrinsic_strategy.federated_broadcast(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_zip_at_server(self, arg):
    return await self.intrinsic_strategy.federated_zip_at_server(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_zip_at_clients(self, arg):
    return await self.intrinsic_strategy.federated_zip_at_clients(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_reduce(self, arg):
    return await self.intrinsic_strategy.federated_reduce(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_aggregate(self, arg):
    return await self.intrinsic_strategy.federated_aggregate(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_sum(self, arg):
    return await self.intrinsic_strategy.federated_sum(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_mean(self, arg):
    return await self.intrinsic_strategy.federated_mean(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_weighted_mean(self, arg):
    return await executor_utils.compute_federated_weighted_mean(self, arg)

  @tracing.trace
  async def _compute_intrinsic_federated_collect(self, arg):
    return await self.intrinsic_strategy.federated_collect(arg)

  @tracing.trace
  async def _compute_intrinsic_federated_secure_sum(self, arg):
    return await self.intrinsic_strategy.federated_secure_sum(arg)
