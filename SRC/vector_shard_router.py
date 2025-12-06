
"""Vector DB/feature store sharding using the consistent ring.
Storage placement via get_node; demo query is scatter-gather.
No hooks: rebalance via rebalance.py.
"""
from __future__ import annotations

from typing import List, Dict, Optional
from consistent_hash_ring import ConsistentHasher

class VectorShard:
    def __init__(self, shard_id: str):
        self.shard_id = shard_id
        self._vecs: Dict[str, List[float]] = {}

    def upsert(self, vec_id: str, vec: List[float]):
        self._vecs[vec_id] = vec

    def get(self, vec_id: str) -> Optional[List[float]]:
        return self._vecs.get(vec_id)

    def search(self, query: List[float], top_k: int = 5) -> List[tuple[str, float]]:
        import math
        def cos(a, b):
            dot = sum(x*y for x,y in zip(a,b))
            na = math.sqrt(sum(x*x for x in a)) + 1e-12
            nb = math.sqrt(sum(x*x for x in b)) + 1e-12
            return dot / (na * nb)
        scored = [(vid, cos(query, v)) for vid, v in self._vecs.items()]
        return sorted(scored, key=lambda x: -x[1])[:top_k]

class VectorRouter:
    def __init__(self, ring: ConsistentHasher, replication: int = 1):
        self.ring = ring
        self.replication = replication
        self._shards: Dict[str, VectorShard] = {}

    def attach_shard(self, shard_id: str, shard: VectorShard, weight: int = 1):
        self._shards[shard_id] = shard
        self.ring.add_node(shard_id, weight=weight)

    def detach_shard(self, shard_id: str):
        self._shards.pop(shard_id, None)
        self.ring.remove_node(shard_id)

    def _placement(self, vec_id: str) -> List[VectorShard]:
        node_ids = self.ring.get_nodes_for_key(vec_id, replicas=self.replication, multiprobe=2)
        return [self._shards[nid] for nid in node_ids if nid in self._shards]

    def placement_with_ring(self, vec_id: str, ring: ConsistentHasher) -> List[VectorShard]:
        node_ids = ring.get_nodes_for_key(vec_id, replicas=self.replication, multiprobe=2)
        return [self._shards.get(nid) for nid in node_ids if nid in self._shards]
