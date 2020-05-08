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

import asyncio
from typing import Tuple

from absl.testing import absltest
from absl.testing import parameterized
import tensorflow as tf

from tensorflow_federated.proto.v0 import computation_pb2 as pb
from tensorflow_federated.python.common_libs import anonymous_tuple
from tensorflow_federated.python.core.api import computation_types
from tensorflow_federated.python.core.api import computations
from tensorflow_federated.python.core.api import intrinsics
from tensorflow_federated.python.core.impl import computation_impl
from tensorflow_federated.python.core.impl import intrinsic_factory
from tensorflow_federated.python.core.impl.compiler import building_block_factory
from tensorflow_federated.python.core.impl.compiler import building_blocks
from tensorflow_federated.python.core.impl.compiler import intrinsic_defs
from tensorflow_federated.python.core.impl.compiler import placement_literals
from tensorflow_federated.python.core.impl.compiler import type_factory
from tensorflow_federated.python.core.impl.compiler import type_serialization
from tensorflow_federated.python.core.impl.context_stack import context_stack_impl
from tensorflow_federated.python.core.impl.executors import eager_tf_executor
from tensorflow_federated.python.core.impl.executors import executor_factory
from tensorflow_federated.python.core.impl.executors import executor_test_utils
from tensorflow_federated.python.core.impl.executors import federating_executor
from tensorflow_federated.python.core.impl.executors import reference_resolving_executor

tf.compat.v1.enable_v2_behavior()


def create_test_executor(
    num_clients=1,
    use_reference_resolving_executor=False,
    intrinsic_strategy_fn=federating_executor.CentralizedIntrinsicStrategy,
) -> federating_executor.FederatingExecutor:
  bottom_ex = eager_tf_executor.EagerTFExecutor()
  if use_reference_resolving_executor:
    bottom_ex = reference_resolving_executor.ReferenceResolvingExecutor(
        bottom_ex)
  target_executors = {
      placement_literals.SERVER: bottom_ex,
      placement_literals.CLIENTS: [bottom_ex] * num_clients,
      None: bottom_ex,
  }
  if intrinsic_strategy_fn is federating_executor.TrustedAggregatorIntrinsicStrategy:
    target_executors[placement_literals.AGGREGATORS] = bottom_ex
  return federating_executor.FederatingExecutor(
      target_executors, intrinsic_strategy_fn=intrinsic_strategy_fn)


def create_test_executor_factory(
    intrinsic_strategy_fn=federating_executor.CentralizedIntrinsicStrategy):
  executor = create_test_executor(
      num_clients=1, intrinsic_strategy_fn=intrinsic_strategy_fn)
  return executor_factory.ExecutorFactoryImpl(lambda _: executor)


Runtime = Tuple[asyncio.AbstractEventLoop,
                federating_executor.FederatingExecutor]


def _make_test_runtime(
    num_clients=1,
    use_reference_resolving_executor=False,
    intrinsic_strategy_fn=federating_executor.CentralizedIntrinsicStrategy,
) -> Runtime:
  """Creates a test runtime consisting of an event loop and test executor."""
  loop = asyncio.get_event_loop()
  ex = create_test_executor(
      num_clients=num_clients,
      use_reference_resolving_executor=use_reference_resolving_executor,
      intrinsic_strategy_fn=intrinsic_strategy_fn)
  return loop, ex


def _run_comp_with_runtime(comp, runtime: Runtime):
  """Runs a computation using the provided runtime."""
  loop, ex = runtime

  async def call_value():
    return await ex.create_call(await ex.create_value(comp))

  return loop.run_until_complete(call_value())


def _run_test_comp(
    comp,
    num_clients=1,
    use_reference_resolving_executor=False,
    intrinsic_strategy_fn=federating_executor.CentralizedIntrinsicStrategy):
  """Runs a computation (unapplied TFF function) using a test runtime."""
  runtime = _make_test_runtime(
      num_clients=num_clients,
      use_reference_resolving_executor=use_reference_resolving_executor,
      intrinsic_strategy_fn=intrinsic_strategy_fn)
  return _run_comp_with_runtime(comp, runtime)


def _run_test_comp_produces_federated_value(
    test_instance,
    comp,
    num_clients=1,
    use_reference_resolving_executor=False,
    intrinsic_strategy_fn=federating_executor.CentralizedIntrinsicStrategy,
):
  """Runs a computation (unapplied TFF function) using a test runtime.

  This is similar to _run_test_comp, but the result is asserted to be a
  FederatedValue and computed.

  Args:
    test_instance: A class with the standard unit testing assertions.
    comp: The computation to run.
    num_clients: The number of clients to use when computing `comp`.
    use_reference_resolving_executor: Whether or not to include an executor
      to resolve references.

  Returns:
    The result of running the computation.
  """
  loop, ex = _make_test_runtime(
      num_clients=num_clients,
      use_reference_resolving_executor=use_reference_resolving_executor,
      intrinsic_strategy_fn=intrinsic_strategy_fn)
  val = _run_comp_with_runtime(comp, (loop, ex))
  test_instance.assertIsInstance(val,
                                 federating_executor.FederatingExecutorValue)
  return loop.run_until_complete(val.compute())


def _produce_test_value(
    value,
    type_spec=None,
    num_clients=1,
    use_reference_resolving_executor=False,
    intrinsic_strategy_fn=federating_executor.CentralizedIntrinsicStrategy,
):
  """Produces a TFF value using a test runtime."""
  loop, ex = _make_test_runtime(
      num_clients=num_clients,
      use_reference_resolving_executor=use_reference_resolving_executor,
      intrinsic_strategy_fn=intrinsic_strategy_fn)
  return loop.run_until_complete(ex.create_value(value, type_spec=type_spec))


class FederatingExecutorCreateValueTest(executor_test_utils.AsyncTestCase,
                                        parameterized.TestCase):

  # pyformat: disable
  @parameterized.named_parameters([
      ('intrinsic_def', *executor_test_utils.create_dummy_intrinsic_def()),
      ('placement_literal',
       *executor_test_utils.create_dummy_placement_literal()),
      ('computation_impl',
       *executor_test_utils.create_dummy_computation_impl()),
      ('computation_call',
       *executor_test_utils.create_dummy_computation_call()),
      ('computation_intrinsic',
       *executor_test_utils.create_dummy_computation_intrinsic()),
      ('computation_lambda',
       *executor_test_utils.create_dummy_computation_lambda_empty()),
      ('computation_placement',
       *executor_test_utils.create_dummy_computation_placement()),
      ('computation_selection',
       *executor_test_utils.create_dummy_computation_selection()),
      ('computation_tensorflow',
       *executor_test_utils.create_dummy_computation_tensorflow_empty()),
      ('computation_tuple',
       *executor_test_utils.create_dummy_computation_tuple()),
      ('federated_type_clients',
       *executor_test_utils.create_dummy_value_clients()),
      ('federated_type_clients_all_equal',
       *executor_test_utils.create_dummy_value_clients_all_equal()),
      ('federated_type_server',
       *executor_test_utils.create_dummy_value_server()),
      ('unplaced_type', *executor_test_utils.create_dummy_value_unplaced()),
  ])
  # pyformat: enable
  def test_returns_value_with_value_and_type(self, value, type_signature):
    executor = create_test_executor(num_clients=3)

    result = self.run_sync(executor.create_value(value, type_signature))

    self.assertIsInstance(result, federating_executor.FederatingExecutorValue)
    self.assertEqual(result.type_signature.compact_representation(),
                     type_signature.compact_representation())

  # pyformat: disable
  @parameterized.named_parameters([
      ('placement_literal',
       *executor_test_utils.create_dummy_placement_literal()),
      ('computation_impl',
       *executor_test_utils.create_dummy_computation_impl()),
      ('computation_call',
       *executor_test_utils.create_dummy_computation_call()),
      ('computation_intrinsic',
       *executor_test_utils.create_dummy_computation_intrinsic()),
      ('computation_lambda',
       *executor_test_utils.create_dummy_computation_lambda_empty()),
      ('computation_placement',
       *executor_test_utils.create_dummy_computation_placement()),
      ('computation_selection',
       *executor_test_utils.create_dummy_computation_selection()),
      ('computation_tensorflow',
       *executor_test_utils.create_dummy_computation_tensorflow_empty()),
      ('computation_tuple',
       *executor_test_utils.create_dummy_computation_tuple()),
  ])
  # pyformat: enable
  def test_returns_value_with_value_only(self, value, type_signature):
    executor = create_test_executor(num_clients=3)

    result = self.run_sync(executor.create_value(value))

    self.assertIsInstance(result, federating_executor.FederatingExecutorValue)
    self.assertEqual(result.type_signature.compact_representation(),
                     type_signature.compact_representation())

  # pyformat: disable
  @parameterized.named_parameters([
      ('intrinsic_def', *executor_test_utils.create_dummy_intrinsic_def()),
      ('federated_type_clients',
       *executor_test_utils.create_dummy_value_clients()),
      ('federated_type_clients_all_equal',
       *executor_test_utils.create_dummy_value_clients_all_equal()),
      ('federated_type_server',
       *executor_test_utils.create_dummy_value_server()),
      ('unplaced_type', *executor_test_utils.create_dummy_value_unplaced()),
  ])
  # pyformat: enable
  def test_raises_type_error_with_value_only(self, value, _):
    executor = create_test_executor(num_clients=3)

    with self.assertRaises(TypeError):
      self.run_sync(executor.create_value(value))

  # pyformat: disable
  @parameterized.named_parameters([
      ('intrinsic_def', *executor_test_utils.create_dummy_intrinsic_def()),
      ('placement_literal',
       *executor_test_utils.create_dummy_placement_literal()),
      ('computation_impl',
       *executor_test_utils.create_dummy_computation_impl()),
      ('computation_placement',
       *executor_test_utils.create_dummy_computation_placement()),
      ('federated_type_clients',
       *executor_test_utils.create_dummy_value_clients()),
      ('federated_type_clients_all_equal',
       *executor_test_utils.create_dummy_value_clients_all_equal()),
      ('federated_type_server',
       *executor_test_utils.create_dummy_value_server()),
      ('unplaced_type', *executor_test_utils.create_dummy_value_unplaced()),
  ])
  # pyformat: enable
  def test_raises_type_error_with_value_and_bad_type(self, value, _):
    executor = create_test_executor(num_clients=3)
    bad_type_signature = computation_types.TensorType(tf.string)

    with self.assertRaises(TypeError):
      self.run_sync(executor.create_value(value, bad_type_signature))

  # pyformat: disable
  @parameterized.named_parameters([
      ('computation_call',
       *executor_test_utils.create_dummy_computation_call()),
      ('computation_intrinsic',
       *executor_test_utils.create_dummy_computation_intrinsic()),
      ('computation_lambda',
       *executor_test_utils.create_dummy_computation_lambda_empty()),
      ('computation_selection',
       *executor_test_utils.create_dummy_computation_selection()),
      ('computation_tensorflow',
       *executor_test_utils.create_dummy_computation_tensorflow_empty()),
      ('computation_tuple',
       *executor_test_utils.create_dummy_computation_tuple()),
  ])
  # pyformat: enable
  def test_raises_type_error_with_value_and_bad_type_skipped(self, value, _):
    self.skipTest(
        'TODO(b/152449402): `FederatingExecutor.create_value` method should '
        'fail if it is passed a computation and an incompatible type.')
    executor = create_test_executor(num_clients=3)
    bad_type_signature = computation_types.TensorType(tf.string)

    with self.assertRaises(TypeError):
      self.run_sync(executor.create_value(value, bad_type_signature))

  # pyformat: disable
  @parameterized.named_parameters([
      ('computation_reference',
       *executor_test_utils.create_dummy_computation_reference()),
      ('function_type', lambda: 10, type_factory.unary_op(tf.int32)),
  ])
  # pyformat: enable
  def test_raises_value_error_with_value(self, value, type_signature):
    executor = create_test_executor(num_clients=3)

    with self.assertRaises(ValueError):
      self.run_sync(executor.create_value(value, type_signature))

  def test_raises_value_error_with_unrecognized_computation_intrinsic(self):
    executor = create_test_executor(num_clients=3)
    # A `ValueError` will be raised because `create_value` can not recognize the
    # following intrinsic, because it has not been added to the intrinsic
    # registry.
    value = pb.Computation(
        type=type_serialization.serialize_type(tf.int32),
        intrinsic=pb.Intrinsic(uri='unregistered_intrinsic'))
    type_signature = computation_types.TensorType(tf.int32)

    with self.assertRaises(ValueError):
      self.run_sync(executor.create_value(value, type_signature))

  def test_raises_value_error_with_unrecognized_computation_selection(self):
    executor = create_test_executor(num_clients=3)
    source, _ = executor_test_utils.create_dummy_computation_tuple()
    type_signature = computation_types.NamedTupleType([])
    # A `ValueError` will be raised because `create_value` can not handle the
    # following `pb.Selection`, because does not set either a name or an index
    # field.
    value = pb.Computation(
        type=type_serialization.serialize_type(type_signature),
        selection=pb.Selection(source=source))

    with self.assertRaises(ValueError):
      self.run_sync(executor.create_value(value, type_signature))

  def test_raises_value_error_with_no_target_executor_federated_type_clients(
      self):
    # A `ValueError` will be raised because `create_value` can not find a target
    # executor of the appropriate placement, however the following
    # `federating_executor.FederatingExecutor` was created without one.
    executor = federating_executor.FederatingExecutor({
        placement_literals.SERVER: eager_tf_executor.EagerTFExecutor(),
        None: eager_tf_executor.EagerTFExecutor()
    })
    value, type_signature = executor_test_utils.create_dummy_value_clients()

    with self.assertRaises(ValueError):
      self.run_sync(executor.create_value(value, type_signature))

  def test_raises_value_error_with_no_target_executor_federated_type_server(
      self):
    # A `ValueError` will be raised because `create_value` can not find a target
    # executor of the appropriate placement, because the following
    # `federating_executor.FederatingExecutor` was created without one.
    executor = federating_executor.FederatingExecutor({
        placement_literals.CLIENTS: eager_tf_executor.EagerTFExecutor(),
        None: eager_tf_executor.EagerTFExecutor()
    })
    value, type_signature = executor_test_utils.create_dummy_value_server()

    with self.assertRaises(ValueError):
      self.run_sync(executor.create_value(value, type_signature))

  def test_raises_value_error_with_no_target_executor_unplaced_type(self):
    # A `ValueError` will be raised because `create_value` can not find a target
    # executor of the appropriate placement, because the following
    # `federating_executor.FederatingExecutor` was created without one.
    executor = federating_executor.FederatingExecutor({
        placement_literals.SERVER: eager_tf_executor.EagerTFExecutor(),
        placement_literals.CLIENTS: eager_tf_executor.EagerTFExecutor(),
    })
    value, type_signature = executor_test_utils.create_dummy_value_unplaced()

    with self.assertRaises(ValueError):
      self.run_sync(executor.create_value(value, type_signature))

  def test_raises_value_error_with_unexpected_federated_type_clients(self):
    executor = create_test_executor(num_clients=3)
    value = [10, 20]
    type_signature = type_factory.at_clients(tf.int32)

    with self.assertRaises(ValueError):
      self.run_sync(executor.create_value(value, type_signature))

  def test_raises_value_error_with_unexpected_federated_type_clients_all_equal(
      self):
    executor = create_test_executor(num_clients=3)
    value = [10, 10, 10]
    type_signature = type_factory.at_clients(tf.int32, all_equal=True)

    with self.assertRaises(ValueError):
      self.run_sync(executor.create_value(value, type_signature))


class FederatingExecutorCreateCallTest(executor_test_utils.AsyncTestCase,
                                       parameterized.TestCase):

  # pyformat: disable
  @parameterized.named_parameters([
      ('intrinsic_def', *executor_test_utils.create_dummy_intrinsic_def(),
       *executor_test_utils.create_dummy_computation_tensorflow_constant()),
      ('computation_impl', *executor_test_utils.create_dummy_computation_impl(),
       *executor_test_utils.create_dummy_value_unplaced()),
      ('computation_intrinsic',
       *executor_test_utils.create_dummy_computation_intrinsic(),
       *executor_test_utils.create_dummy_computation_tensorflow_constant()),
      ('computation_tensorflow',
       *executor_test_utils.create_dummy_computation_tensorflow_identity(),
       *executor_test_utils.create_dummy_value_unplaced()),
  ])
  # pyformat: enable
  def test_returns_value_with_comp_and_arg(self, comp, comp_type, arg,
                                           arg_type):
    executor = create_test_executor(num_clients=3)

    comp = self.run_sync(executor.create_value(comp, comp_type))
    arg = self.run_sync(executor.create_value(arg, arg_type))
    result = self.run_sync(executor.create_call(comp, arg))

    self.assertIsInstance(result, federating_executor.FederatingExecutorValue)
    self.assertEqual(result.type_signature.compact_representation(),
                     comp_type.result.compact_representation())

  # pyformat: disable
  @parameterized.named_parameters([
      ('computation_lambda',
       *executor_test_utils.create_dummy_computation_lambda_empty()),
      ('computation_tensorflow',
       *executor_test_utils.create_dummy_computation_tensorflow_empty()),
  ])
  # pyformat: enable
  def test_returns_value_with_comp_only(self, comp, comp_type):
    executor = create_test_executor(num_clients=3)

    comp = self.run_sync(executor.create_value(comp, comp_type))
    result = self.run_sync(executor.create_call(comp))

    self.assertIsInstance(result, federating_executor.FederatingExecutorValue)
    self.assertEqual(result.type_signature.compact_representation(),
                     comp_type.result.compact_representation())

  # pyformat: disable
  @parameterized.named_parameters([
      ('intrinsic_def', *executor_test_utils.create_dummy_intrinsic_def()),
      ('computation_impl',
       *executor_test_utils.create_dummy_computation_impl()),
      ('computation_intrinsic',
       *executor_test_utils.create_dummy_computation_intrinsic()),
      ('computation_lambda',
       *executor_test_utils.create_dummy_computation_lambda_identity()),
      ('computation_tensorflow',
       *executor_test_utils.create_dummy_computation_tensorflow_identity()),
  ])
  # pyformat: enable
  def test_raises_type_error_with_comp_and_bad_arg(self, comp, comp_type):
    executor = create_test_executor(num_clients=3)
    bad_arg = 'string'
    bad_arg_type = computation_types.TensorType(tf.string)

    comp = self.run_sync(executor.create_value(comp, comp_type))
    arg = self.run_sync(executor.create_value(bad_arg, bad_arg_type))
    with self.assertRaises(TypeError):
      self.run_sync(executor.create_call(comp, arg))

  def test_raises_type_error_with_unembedded_comp(self):
    executor = create_test_executor(num_clients=3)
    comp, _ = executor_test_utils.create_dummy_computation_tensorflow_identity()
    arg, arg_type = executor_test_utils.create_dummy_value_unplaced()

    arg = self.run_sync(executor.create_value(arg, arg_type))
    with self.assertRaises(TypeError):
      self.run_sync(executor.create_call(comp, arg))

  def test_raises_type_error_with_unembedded_arg(self):
    executor = create_test_executor(num_clients=3)
    comp, comp_type = executor_test_utils.create_dummy_computation_tensorflow_identity(
    )
    arg, _ = executor_test_utils.create_dummy_value_unplaced()

    comp = self.run_sync(executor.create_value(comp, comp_type))
    with self.assertRaises(TypeError):
      self.run_sync(executor.create_call(comp, arg))

  # pyformat: disable
  @parameterized.named_parameters([
      ('computation_call',
       *executor_test_utils.create_dummy_computation_call()),
      ('computation_placement',
       *executor_test_utils.create_dummy_computation_placement()),
      ('computation_selection',
       *executor_test_utils.create_dummy_computation_selection()),
      ('computation_tuple',
       *executor_test_utils.create_dummy_computation_tuple()),
      ('federated_type_clients',
       *executor_test_utils.create_dummy_value_clients()),
      ('federated_type_clients_all_equal',
       *executor_test_utils.create_dummy_value_clients_all_equal()),
      ('federated_type_server',
       *executor_test_utils.create_dummy_value_server()),
      ('unplaced_type', *executor_test_utils.create_dummy_value_unplaced()),
  ])
  # pyformat: enable
  def test_raises_value_error_with_comp(self, comp, comp_type):
    executor = create_test_executor(num_clients=3)

    comp = self.run_sync(executor.create_value(comp, comp_type))
    with self.assertRaises(ValueError):
      self.run_sync(executor.create_call(comp))

  def test_raises_value_error_with_computation_lambda_and_arg(self):
    executor = create_test_executor(num_clients=3)
    comp, comp_type = executor_test_utils.create_dummy_computation_lambda_identity(
    )
    arg, arg_type = executor_test_utils.create_dummy_value_unplaced()

    comp = self.run_sync(executor.create_value(comp, comp_type))
    arg = self.run_sync(executor.create_value(arg, arg_type))
    with self.assertRaises(ValueError):
      self.run_sync(executor.create_call(comp, arg))

  def test_raises_not_implemented_error_with_unimplemented_intrinsic(self):
    executor = create_test_executor(num_clients=3)
    dummy_intrinsic = intrinsic_defs.IntrinsicDef(
        'DUMMY_INTRINSIC', 'dummy_intrinsic',
        computation_types.AbstractType('T'))
    comp = pb.Computation(
        intrinsic=pb.Intrinsic(uri='dummy_intrinsic'),
        type=type_serialization.serialize_type(tf.int32))

    comp = self.run_sync(executor.create_value(comp))
    with self.assertRaises(NotImplementedError):
      self.run_sync(executor.create_call(comp))


class FederatingExecutorCreateTupleTest(executor_test_utils.AsyncTestCase,
                                        parameterized.TestCase):

  # pyformat: disable
  @parameterized.named_parameters([
      ('intrinsic_def', *executor_test_utils.create_dummy_intrinsic_def()),
      ('placement_literal',
       *executor_test_utils.create_dummy_placement_literal()),
      ('computation_impl',
       *executor_test_utils.create_dummy_computation_impl()),
      ('computation_call',
       *executor_test_utils.create_dummy_computation_call()),
      ('computation_intrinsic',
       *executor_test_utils.create_dummy_computation_intrinsic()),
      ('computation_lambda',
       *executor_test_utils.create_dummy_computation_lambda_empty()),
      ('computation_placement',
       *executor_test_utils.create_dummy_computation_placement()),
      ('computation_selection',
       *executor_test_utils.create_dummy_computation_selection()),
      ('computation_tensorflow',
       *executor_test_utils.create_dummy_computation_tensorflow_empty()),
      ('computation_tuple',
       *executor_test_utils.create_dummy_computation_tuple()),
      ('federated_type_clients',
       *executor_test_utils.create_dummy_value_clients()),
      ('federated_type_clients_all_equal',
       *executor_test_utils.create_dummy_value_clients_all_equal()),
      ('federated_type_server',
       *executor_test_utils.create_dummy_value_server()),
      ('unplaced_type', *executor_test_utils.create_dummy_value_unplaced()),
  ])
  # pyformat: enable
  def test_returns_value_with_elements(self, element, type_signature):
    executor = create_test_executor(num_clients=3)

    element = self.run_sync(executor.create_value(element, type_signature))
    elements = [element] * 3
    type_signature = computation_types.NamedTupleType([type_signature] * 3)
    result = self.run_sync(executor.create_tuple(elements))

    self.assertIsInstance(result, federating_executor.FederatingExecutorValue)
    self.assertEqual(result.type_signature.compact_representation(),
                     type_signature.compact_representation())

  def test_raises_type_error_with_unembedded_elements(self):
    executor = create_test_executor(num_clients=3)
    element, _ = executor_test_utils.create_dummy_value_unplaced()

    elements = [element] * 3
    with self.assertRaises(TypeError):
      self.run_sync(executor.create_tuple(elements))


class FederatingExecutorTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_value_at_server(self, strategy):

    @computations.federated_computation
    def comp():
      return intrinsics.federated_value(10, placement_literals.SERVER)

    val = _run_test_comp(comp, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), 'int32@SERVER')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, 1)
    self.assertIsInstance(val.internal_representation[0],
                          eager_tf_executor.EagerValue)
    self.assertEqual(
        val.internal_representation[0].internal_representation.numpy(), 10)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_value_at_client_with_zero_clients_raises_error(
      self, strategy):
    self.skipTest('b/145936344')

    @computations.federated_computation
    def comp():
      return intrinsics.federated_broadcast(
          intrinsics.federated_value(10, placement_literals.SERVER))

    val = _run_test_comp(comp, num_clients=0, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), 'int32@CLIENTS')
    self.assertIsInstance(val.internal_representation, list)
    with self.assertRaisesRegex(RuntimeError, '0 clients'):
      val.compute()

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_value_at_server_with_tuple(self, strategy):

    @computations.federated_computation
    def comp():
      return intrinsics.federated_value([10, 10], placement_literals.SERVER)

    val = _run_test_comp(comp, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), '<int32,int32>@SERVER')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, 1)
    self.assertIsInstance(val.internal_representation[0],
                          eager_tf_executor.EagerValue)
    inner_eager_value = val.internal_representation[0]
    self.assertLen(inner_eager_value.internal_representation, 2)
    self.assertEqual(inner_eager_value.internal_representation[0].numpy(), 10)
    self.assertEqual(inner_eager_value.internal_representation[1].numpy(), 10)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_value_at_clients(self, strategy):

    @computations.federated_computation
    def comp():
      return intrinsics.federated_value(10, placement_literals.CLIENTS)

    val = _run_test_comp(comp, num_clients=3, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), 'int32@CLIENTS')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, 3)
    for v in val.internal_representation:
      self.assertIsInstance(v, eager_tf_executor.EagerValue)
      self.assertEqual(v.internal_representation.numpy(), 10)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_eval_at_clients_simple_number(self, strategy):

    @computations.federated_computation
    def comp():
      return_five = computations.tf_computation(lambda: 5)
      return intrinsics.federated_eval(return_five, placement_literals.CLIENTS)

    num_clients = 3
    val = _run_test_comp(
        comp, num_clients=num_clients, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), '{int32}@CLIENTS')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, num_clients)
    for v in val.internal_representation:
      self.assertIsInstance(v, eager_tf_executor.EagerValue)
      self.assertEqual(v.internal_representation.numpy(), 5)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_eval_at_server_simple_number(self, strategy):

    @computations.federated_computation
    def comp():
      return_five = computations.tf_computation(lambda: 5)
      return intrinsics.federated_eval(return_five, placement_literals.SERVER)

    num_clients = 3
    val = _run_test_comp(
        comp, num_clients=num_clients, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), 'int32@SERVER')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, 1)
    v = val.internal_representation[0]
    self.assertIsInstance(v, eager_tf_executor.EagerValue)
    self.assertEqual(v.internal_representation.numpy(), 5)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_eval_at_clients_random(self, strategy):

    @computations.federated_computation
    def comp():
      rand = computations.tf_computation(lambda: tf.random.normal([]))
      return intrinsics.federated_eval(rand, placement_literals.CLIENTS)

    num_clients = 3
    val = _run_test_comp(
        comp, num_clients=num_clients, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), '{float32}@CLIENTS')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, num_clients)
    previous_values = set()
    for v in val.internal_representation:
      self.assertIsInstance(v, eager_tf_executor.EagerValue)
      number = v.internal_representation.numpy()
      if number in previous_values:
        raise Exception('Multiple clients returned same random number')
      previous_values.add(number)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_map_at_server(self, strategy):
    loop, ex = _make_test_runtime()

    @computations.tf_computation(tf.int32)
    def add_one(x):
      return x + 1

    @computations.federated_computation
    def comp():
      value = intrinsics.federated_value(10, placement_literals.SERVER)
      return intrinsics.federated_map(add_one, value)

    val = _run_comp_with_runtime(comp, (loop, ex))
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), 'int32@SERVER')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, 1)
    v = val.internal_representation[0]
    self.assertIsInstance(v, eager_tf_executor.EagerValue)
    self.assertEqual(v.internal_representation.numpy(), 11)
    result = loop.run_until_complete(v.compute())
    self.assertEqual(result.numpy(), 11)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_map(self, strategy):

    @computations.tf_computation(tf.int32)
    def add_one(x):
      return x + 1

    @computations.federated_computation
    def comp():
      value = intrinsics.federated_value(10, placement_literals.CLIENTS)
      return intrinsics.federated_map(add_one, value)

    val = _run_test_comp(comp, num_clients=3, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), '{int32}@CLIENTS')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, 3)
    for v in val.internal_representation:
      self.assertIsInstance(v, eager_tf_executor.EagerValue)
      self.assertEqual(v.internal_representation.numpy(), 11)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_map_all_equal(self, strategy):
    factory = intrinsic_factory.IntrinsicFactory(
        context_stack_impl.context_stack)

    @computations.tf_computation(tf.int32)
    def add_one(x):
      return x + 1

    @computations.federated_computation
    def comp():
      value = intrinsics.federated_value(10, placement_literals.CLIENTS)
      return factory.federated_map_all_equal(add_one, value)

    val = _run_test_comp(comp, num_clients=3, intrinsic_strategy_fn=strategy)

    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(val.type_signature.compact_representation(),
                     'int32@CLIENTS')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, 3)
    for v in val.internal_representation:
      self.assertIsInstance(v, eager_tf_executor.EagerValue)
      self.assertEqual(v.internal_representation.numpy(), 11)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_broadcast(self, strategy):

    @computations.federated_computation
    def comp():
      return intrinsics.federated_broadcast(
          intrinsics.federated_value(10, placement_literals.SERVER))

    val = _run_test_comp(comp, num_clients=3, intrinsic_strategy_fn=strategy)
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    self.assertEqual(str(val.type_signature), 'int32@CLIENTS')
    self.assertIsInstance(val.internal_representation, list)
    self.assertLen(val.internal_representation, 3)
    for v in val.internal_representation:
      self.assertIsInstance(v, eager_tf_executor.EagerValue)
      self.assertEqual(v.internal_representation.numpy(), 10)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_zip(self, strategy):
    loop, ex = _make_test_runtime(num_clients=3, intrinsic_strategy_fn=strategy)

    @computations.federated_computation
    def ten_on_server():
      return intrinsics.federated_value(10, placement_literals.SERVER)

    @computations.federated_computation
    def ten_on_clients():
      return intrinsics.federated_value(10, placement_literals.CLIENTS)

    for ten, type_string, cardinality, expected_result in [
        (ten_on_server, '<int32,int32>@SERVER', 1, '<10,10>'),
        (ten_on_clients, '{<int32,int32>}@CLIENTS', 3, ['<10,10>'] * 3)
    ]:
      comp = building_block_factory.create_zip_two_values(
          building_blocks.Tuple([
              building_blocks.Call(
                  building_blocks.ComputationBuildingBlock.from_proto(
                      computation_impl.ComputationImpl.get_proto(ten)))
          ] * 2))
      val = loop.run_until_complete(
          ex.create_value(comp.proto, comp.type_signature))
      self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
      self.assertEqual(str(val.type_signature), type_string)
      self.assertIsInstance(val.internal_representation, list)
      self.assertLen(val.internal_representation, cardinality)
      result = loop.run_until_complete(val.compute())

      def _print(x):
        return str(anonymous_tuple.map_structure(lambda v: v.numpy(), x))

      if isinstance(expected_result, list):
        self.assertCountEqual([_print(x) for x in result], expected_result)
      else:
        self.assertEqual(_print(result), expected_result)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_reduce_with_simple_integer_sum(self, strategy):

    @computations.tf_computation(tf.int32, tf.int32)
    def add_numbers(x, y):
      return x + y

    @computations.federated_computation
    def comp():
      return intrinsics.federated_reduce(
          intrinsics.federated_value(10, placement_literals.CLIENTS), 0,
          add_numbers)

    result = _run_test_comp_produces_federated_value(
        self, comp, num_clients=3, intrinsic_strategy_fn=strategy)
    self.assertEqual(result.numpy(), 30)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_aggregate_with_simple_integer_sum(self, strategy):

    @computations.tf_computation(tf.int32, tf.int32)
    def add_numbers(x, y):
      return x + y

    @computations.tf_computation(tf.int32)
    def add_one_because_why_not(x):
      return x + 1

    @computations.federated_computation
    def comp():
      x = intrinsics.federated_value(10, placement_literals.CLIENTS)
      return intrinsics.federated_aggregate(x, 0, add_numbers, add_numbers,
                                            add_one_because_why_not)

    result = _run_test_comp_produces_federated_value(
        self, comp, num_clients=3, intrinsic_strategy_fn=strategy)
    self.assertEqual(result.numpy(), 31)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_sum_with_integers(self, strategy):

    @computations.federated_computation
    def comp():
      x = intrinsics.federated_value(10, placement_literals.CLIENTS)
      return intrinsics.federated_sum(x)

    result = _run_test_comp_produces_federated_value(
        self, comp, num_clients=3, intrinsic_strategy_fn=strategy)
    self.assertEqual(result.numpy(), 30)


class FederatingExecutorTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_mean_with_floats(self, strategy):
    loop, ex = _make_test_runtime(num_clients=4, intrinsic_strategy_fn=strategy)

    v1 = loop.run_until_complete(
        ex.create_value([1.0, 2.0, 3.0, 4.0],
                        type_factory.at_clients(tf.float32)))
    self.assertEqual(str(v1.type_signature), '{float32}@CLIENTS')

    v2 = loop.run_until_complete(
        ex.create_value(
            intrinsic_defs.FEDERATED_MEAN,
            computation_types.FunctionType(
                type_factory.at_clients(tf.float32),
                type_factory.at_server(tf.float32))))
    self.assertEqual(
        str(v2.type_signature), '({float32}@CLIENTS -> float32@SERVER)')

    v3 = loop.run_until_complete(ex.create_call(v2, v1))
    self.assertEqual(str(v3.type_signature), 'float32@SERVER')

    result = loop.run_until_complete(v3.compute())
    self.assertEqual(result.numpy(), 2.5)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_federated_weighted_mean_with_floats(self, strategy):
    loop, ex = _make_test_runtime(
        num_clients=4,
        use_reference_resolving_executor=True,
        intrinsic_strategy_fn=strategy)

    v1 = loop.run_until_complete(
        ex.create_value([1.0, 2.0, 3.0, 4.0],
                        type_factory.at_clients(tf.float32)))
    self.assertEqual(str(v1.type_signature), '{float32}@CLIENTS')

    v2 = loop.run_until_complete(
        ex.create_value([5.0, 10.0, 3.0, 2.0],
                        type_factory.at_clients(tf.float32)))
    self.assertEqual(str(v2.type_signature), '{float32}@CLIENTS')

    v3 = loop.run_until_complete(
        ex.create_tuple(
            anonymous_tuple.AnonymousTuple([(None, v1), (None, v2)])))
    self.assertEqual(
        str(v3.type_signature), '<{float32}@CLIENTS,{float32}@CLIENTS>')

    v4 = loop.run_until_complete(
        ex.create_value(
            intrinsic_defs.FEDERATED_WEIGHTED_MEAN,
            computation_types.FunctionType([
                type_factory.at_clients(tf.float32),
                type_factory.at_clients(tf.float32)
            ], type_factory.at_server(tf.float32))))
    self.assertEqual(
        str(v4.type_signature),
        '(<{float32}@CLIENTS,{float32}@CLIENTS> -> float32@SERVER)')

    v5 = loop.run_until_complete(ex.create_call(v4, v3))
    self.assertEqual(str(v5.type_signature), 'float32@SERVER')

    result = loop.run_until_complete(v5.compute())
    self.assertAlmostEqual(result.numpy(), 2.1, places=3)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_execution_of_tensorflow(self, strategy):

    @computations.tf_computation
    def comp():
      return tf.math.add(5, 5)

    executor = create_test_executor_factory(intrinsic_strategy_fn=strategy)
    with executor_test_utils.install_executor(executor):
      result = comp()

    self.assertEqual(result, 10)

  @parameterized.named_parameters(
      (('_'.join([x[0], y[0]]), x[1], y[1])
       for x in [('tuple', (1, 2, 3, 4)), (
           'set', set([1, 2, 3, 4])), ('frozenset', frozenset([1, 2, 3, 4]))]
       for y in [('centralized',
                  federating_executor.CentralizedIntrinsicStrategy),
                 ('trusted_aggregator',
                  federating_executor.TrustedAggregatorIntrinsicStrategy)]))
  def test_with_federated_value_as_a_non_py_list(self, val, strategy):
    loop, ex = _make_test_runtime(num_clients=4, intrinsic_strategy_fn=strategy)
    v = loop.run_until_complete(
        ex.create_value(val, type_factory.at_clients(tf.int32)))
    self.assertEqual(str(v.type_signature), '{int32}@CLIENTS')
    result = tf.nest.map_structure(lambda x: x.numpy(),
                                   loop.run_until_complete(v.compute()))
    self.assertCountEqual(result, [1, 2, 3, 4])

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_create_selection_by_index_anonymous_tuple_backed(self, strategy):
    loop = asyncio.get_event_loop()
    ex = create_test_executor(num_clients=4, intrinsic_strategy_fn=strategy)

    v1 = loop.run_until_complete(
        ex.create_value([1.0, 2.0, 3.0, 4.0],
                        type_factory.at_clients(tf.float32)))
    self.assertEqual(str(v1.type_signature), '{float32}@CLIENTS')

    v2 = loop.run_until_complete(
        ex.create_value([5.0, 10.0, 3.0, 2.0],
                        type_factory.at_clients(tf.float32)))
    self.assertEqual(str(v2.type_signature), '{float32}@CLIENTS')

    v3 = loop.run_until_complete(
        ex.create_tuple(
            anonymous_tuple.AnonymousTuple([(None, v1), (None, v2)])))
    self.assertEqual(
        str(v3.type_signature), '<{float32}@CLIENTS,{float32}@CLIENTS>')

    v4 = loop.run_until_complete(ex.create_selection(v3, index=0))
    self.assertEqual(str(v4.type_signature), '{float32}@CLIENTS')
    result = tf.nest.map_structure(lambda x: x.numpy(),
                                   loop.run_until_complete(v4.compute()))
    self.assertCountEqual(result, [1, 2, 3, 4])

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_create_selection_by_name_anonymous_tuple_backed(self, strategy):
    loop, ex = _make_test_runtime(num_clients=4, intrinsic_strategy_fn=strategy)

    v1 = loop.run_until_complete(
        ex.create_value([1.0, 2.0, 3.0, 4.0],
                        type_factory.at_clients(tf.float32)))
    self.assertEqual(str(v1.type_signature), '{float32}@CLIENTS')

    v2 = loop.run_until_complete(
        ex.create_value([5.0, 10.0, 3.0, 2.0],
                        type_factory.at_clients(tf.float32)))
    self.assertEqual(str(v2.type_signature), '{float32}@CLIENTS')

    v3 = loop.run_until_complete(
        ex.create_tuple(anonymous_tuple.AnonymousTuple([('a', v1), ('b', v2)])))
    self.assertEqual(
        str(v3.type_signature), '<a={float32}@CLIENTS,b={float32}@CLIENTS>')

    v4 = loop.run_until_complete(ex.create_selection(v3, name='b'))
    self.assertEqual(str(v4.type_signature), '{float32}@CLIENTS')
    result = tf.nest.map_structure(lambda x: x.numpy(),
                                   loop.run_until_complete(v4.compute()))
    self.assertCountEqual(result, [5, 10, 3, 2])

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_create_selection_by_index_eager_tf_executor_backed(self, strategy):
    loop, ex = _make_test_runtime(intrinsic_strategy_fn=strategy)

    @computations.tf_computation()
    def comp():
      return (1, 2)

    val = loop.run_until_complete(ex.create_value(comp))
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    v1 = loop.run_until_complete(ex.create_call(val, None))
    self.assertEqual(str(v1.type_signature), '<int32,int32>')
    selected = loop.run_until_complete(ex.create_selection(v1, index=0))
    self.assertEqual(str(selected.type_signature), 'int32')
    result = loop.run_until_complete(selected.compute())
    self.assertEqual(result, 1)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_create_selection_by_index_reference_resolving_executor_backed(
      self, strategy):
    loop, ex = _make_test_runtime(
        use_reference_resolving_executor=True, intrinsic_strategy_fn=strategy)

    @computations.tf_computation()
    def comp():
      return (1, 2)

    val = loop.run_until_complete(ex.create_value(comp))
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    v1 = loop.run_until_complete(ex.create_call(val, None))
    self.assertEqual(str(v1.type_signature), '<int32,int32>')
    selected = loop.run_until_complete(ex.create_selection(v1, index=0))
    self.assertEqual(str(selected.type_signature), 'int32')
    result = loop.run_until_complete(selected.compute())
    self.assertEqual(result, 1)

  @parameterized.named_parameters(
      ('centralized', federating_executor.CentralizedIntrinsicStrategy),
      ('trusted_aggregator',
       federating_executor.TrustedAggregatorIntrinsicStrategy),
  )
  def test_create_selection_by_name_eager_tf_executor_backed(self, strategy):
    loop, ex = _make_test_runtime(intrinsic_strategy_fn=strategy)

    @computations.tf_computation()
    def comp():
      return anonymous_tuple.AnonymousTuple([('a', 1), ('b', 2)])

    val = loop.run_until_complete(ex.create_value(comp))
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    v1 = loop.run_until_complete(ex.create_call(val, None))
    self.assertEqual(str(v1.type_signature), '<a=int32,b=int32>')
    selected = loop.run_until_complete(ex.create_selection(v1, name='b'))
    self.assertEqual(str(selected.type_signature), 'int32')
    result = loop.run_until_complete(selected.compute())
    self.assertEqual(result, 2)

  def test_federated_collect(self):
    loop, ex = _make_test_runtime(num_clients=3)

    @computations.federated_computation
    def comp():
      x = intrinsics.federated_value(10, placement_literals.CLIENTS)
      return intrinsics.federated_collect(x)

    val = _run_comp_with_runtime(comp, (loop, ex))
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    result = loop.run_until_complete(val.compute())
    self.assertEqual([x.numpy() for x in result], [10, 10, 10])

    new_ex = create_test_executor(num_clients=5)
    val = _run_comp_with_runtime(comp, (loop, new_ex))
    self.assertIsInstance(val, federating_executor.FederatingExecutorValue)
    result = loop.run_until_complete(val.compute())
    self.assertEqual([x.numpy() for x in result], [10, 10, 10, 10, 10])

  def test_federated_collect_trusted_aggregator_fails(self):
    strategy = federating_executor.TrustedAggregatorIntrinsicStrategy
    loop, ex = _make_test_runtime(num_clients=3, intrinsic_strategy_fn=strategy)

    @computations.federated_computation
    def comp():
      x = intrinsics.federated_value(10, placement_literals.CLIENTS)
      return intrinsics.federated_collect(x)

    with self.assertRaises(NotImplementedError):
      val = _run_comp_with_runtime(comp, (loop, ex))

  def test_federated_collect_with_map_call(self):

    @computations.tf_computation()
    def make_dataset():
      return tf.data.Dataset.range(5)

    @computations.tf_computation(computation_types.SequenceType(tf.int64))
    def foo(x):
      return x.reduce(tf.constant(0, dtype=tf.int64), lambda a, b: a + b)

    @computations.federated_computation()
    def bar():
      x = intrinsics.federated_value(make_dataset(), placement_literals.CLIENTS)
      return intrinsics.federated_map(
          foo, intrinsics.federated_collect(intrinsics.federated_map(foo, x)))

    result = _run_test_comp_produces_federated_value(self, bar, num_clients=5)
    self.assertEqual(result.numpy(), 50)


class IntrinsicStrategyTest(parameterized.TestCase):

  def test_improper_intrinsic_strategy_fn(self):

    class MockIntrinsicStrategy:

      def __init__(self, parent_executor):
        self.executor = parent_executor

      @classmethod
      def validate_target_executors(cls, target_executors):
        pass

    with self.assertRaises(TypeError):
      create_test_executor(intrinsic_strategy_fn=MockIntrinsicStrategy)

  def test_placement_validate_necessary(self):

    class MockIntrinsicStrategy(federating_executor.IntrinsicStrategy):

      def __init__(self, parent_executor):
        self.executor = parent_executor

    with self.assertRaises(TypeError):
      create_test_executor(intrinsic_strategy_fn=MockIntrinsicStrategy)


# class EncryptionTest(parameterized.TestCase):

#   def test_generate_aggregator_keys(self):
#     strategy = federating_executor.TrustedAggregatorIntrinsicStrategy
#     loop, ex = _make_test_runtime(intrinsic_strategy_fn=strategy)
#     generate_keys = ex.intrinsic_strategy._trusted_aggregator_generate_keys()
#     pk, sk = loop.run_until_complete(generate_keys)

#     self.assertEqual(str(pk.type_signature), 'uint8[32]@CLIENTS')
#     self.assertEqual(str(sk.type_signature), 'uint8[32]@AGGREGATORS')

#   def test_encryption_decryption(self):

#     strategy = federating_executor.TrustedAggregatorIntrinsicStrategy
#     loop, ex = _make_test_runtime(intrinsic_strategy_fn=strategy)
#     strat_ex = ex.intrinsic_strategy

#     pk_a, sk_a = loop.run_until_complete(
#         strat_ex._trusted_aggregator_generate_keys())

#     val = loop.run_until_complete(
#         ex.create_value([2.0], type_factory.at_clients(tf.float32)))

#     val_enc = loop.run_until_complete(
#         strat_ex._encrypt_client_tensors(val, pk_a))

#     aggr = strat_ex._get_child_executors(
#         placement_literals.AGGREGATORS, index=0)

#     enc_val_on_aggr = loop.run_until_complete(
#         strat_ex._move(val_enc.internal_representation[0],
#                        val_enc.type_signature.member, aggr))

#     val_key_zipped = loop.run_until_complete(
#         strat_ex._zip_val_key([enc_val_on_aggr], sk_a,
#                               placement_literals.AGGREGATORS))

#     val_dec = loop.run_until_complete(
#         strat_ex._decrypt_tensors_on_aggregator(val_key_zipped, tf.float32))

#     dec_tf_tensor = val_dec.internal_representation[0].internal_representation

#     self.assertEqual(dec_tf_tensor, tf.constant(2.0, dtype=tf.float32))

if __name__ == '__main__':
  absltest.main()
