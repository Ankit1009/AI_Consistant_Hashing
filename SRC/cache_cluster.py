
"""Distributed cache adapter using the consistent ring for placement.
Demo node implements LRU+TTL. Swap with Redis/Memcached in production.
No hooks: rebalance is handled via rebalance.py.
"""
from __future__ import annotations

from typing import Optional, Any, Dict, Tuple, List
from collections import OrderedDict
import time
import threading

from consistent_hash_ring import ConsistentHasher

class InMemoryCacheNode:
    def __init__(self, node_id: str, capacity_items: int = 100_000):
        self.node_id = node_id
        self._lock = threading.Lock()
        self._store: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._capacity = capacity_items

    def _evict_if_needed(self) -> None:
        while len(self._store) > self._capacity:
            self._store.popitem(last=False)

    def get(self, k: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            entry = self._store.get(k)
            if not entry:
                return None
            exp_ts, val = entry
            if exp_ts and exp_ts < now:
                self._store.pop(k, None)
                return None
            self._store.move_to_end(k, last=True)
            return val

    def set(self, k: str, v: Any, ttl_sec: int = 600) -> None:
        exp_ts = time.time() + ttl_sec if ttl_sec > 0 else float("inf")
        with self._lock:
            self._store[k] = (exp_ts, v)
            self._store.move_to_end(k, last=True)
            self._evict_if_needed()

class DistributedCache:
    def __init__(self, ring: ConsistentHasher, replication: int = 2):
        self.ring = ring
        self.replication = replication
        self._nodes: Dict[str, InMemoryCacheNode] = {}

    def attach_node(self, node_id: str, impl: InMemoryCacheNode, weight: int = 1):
        self._nodes[node_id] = impl
        self.ring.add_node(node_id, weight=weight)

    def detach_node(self, node_id: str):
        self._nodes.pop(node_id, None)
        self.ring.remove_node(node_id)

    def _placement(self, key: str) -> List[InMemoryCacheNode]:
        node_ids = self.ring.get_nodes_for_key(key, replicas=self.replication, multiprobe=2)
        return [self._nodes[nid] for nid in node_ids if nid in self._nodes]

    def placement_with_ring(self, key: str, ring: ConsistentHasher) -> List[InMemoryCacheNode]:
        node_ids = ring.get_nodes_for_key(key, replicas=self.replication, multiprobe=2)
        return [self._nodes.get(nid) for nid in node_ids if nid in self._nodes]

    def get(self, key: str) -> Optional[Any]:
        for node in self._placement(key):
            val = node.get(key)
            if val is not None:
                return val
        return None

    def set(self, key: str, value: Any, ttl_sec: int = 600):
        for node in self._placement(key):
            node.set(key, value, ttl_sec)

    def node_impl(self, node_id: str) -> Optional[InMemoryCacheNode]:
        return self._nodes.get(node_id)
