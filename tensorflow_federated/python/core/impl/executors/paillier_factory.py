from dataclasses import dataclass

import tensorflow as tf
from tensorflow_federated.python.core.impl.executors import executor_factory
from tensorflow_federated.python.core.impl.executors import executor_stacks

import paillier_placement
import paillier_strategy


def paillier_executor_factory(
    num_clients=None,
    server_tf_device=None,
    aggregator_tf_device=None,
    client_tf_devices=tuple(),
) -> executor_factory.ExecutorFactory:
  pass


def _create_paillier_federated_stack(num_clients, num_client_executors,
    paillier_device_scheduler):
  client_bottom_stacks = [
      executor_stacks._create_bottom_stack(
          device=device_scheduler.next_client_device())
      for _ in range(num_client_executors)
  ]
  executor_dict = {
      placement_literals.CLIENTS: [
          client_bottom_stacks[k % len(client_bottom_stacks)]
          for k in range(num_clients)
      ],
      placement_literals.SERVER: executor_stacks._create_bottom_stack(
          device=device_scheduler.server_device()),
      pallier_placement.PAILLIER: executor_stacks._create_bottom_stack(
          device=device_scheduler.aggregator_device()),
      None: executor_stacks._create_bottom_stack(
          device=device_scheduler.server_device()),
  }
  return federating_executor.FederatingExecutor(
      executor_dict, intrinsic_strategy_fn=paillier_strategy.PaillierStrategy)


class _PaillierDeviceScheduler(executor_stacks._DeviceScheduler):
  """Assign server and clients to devices. Useful in multi-GPU environment."""

  def __init__(self, server_tf_device, aggregator_tf_device, client_tf_devices):
    """Initialize with server and client TF device placement.

    Args:
      server_tf_device: A `tf.config.LogicalDevice` to place server and other
        computation without explicit TFF placement.
      aggregator_tf_device: A `tf.config.LogicalDevice` to place Paillier
        aggregator computations. This is currently required to be CPU device
        by the Paillier primitives.
      client_tf_devices: List/tuple of `tf.config.LogicalDevice` to place
        clients for simulation. Possibly accelerators returned by
        `tf.config.list_logical_devices()`.
    """
    super().__init__(self, server_tf_device, client_tf_devices)
    if aggregator_tf_device is None:
      self._aggregator_device = None
    else:
      py_typecheck.check_type(aggregator_tf_device, tf.config.LogicalDevice)
      self._aggregator_device = aggregator_tf_device.name
  
  def aggregator_device(self):
    return self._aggregator_device
