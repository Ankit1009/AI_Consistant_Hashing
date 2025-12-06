
"""Rebalancing utilities.

Plan owner changes for keys/IDs/artifacts and execute warmup/backfill accordingly.
"""
from __future__ import annotations

from typing import Iterable, Dict, Tuple, List, Optional
from collections import Counter

from consistent_hash_ring import ConsistentHasher
from cache_cluster import DistributedCache, InMemoryCacheNode
from vector_shard_router import VectorRouter, VectorShard
from artifact_placement import ArtifactDistributor, ArtifactHost

class RebalancePlanner:
    def plan_moved(self, keys: Iterable[str], ring_before: ConsistentHasher, ring_after: ConsistentHasher) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
        """Return dict key -> (from_owner, to_owner) for keys whose primary owner changed."""
        moved = {}
        for k in keys:
            b = ring_before.get_node(k)
            a = ring_after.get_node(k)
            if b != a:
                moved[k] = (b, a)
        return moved

    def stats(self, plan: Dict[str, Tuple[Optional[str], Optional[str]]]) -> Dict[str, float]:
        by_to = Counter([to for (_, to) in plan.values() if to is not None])
        by_from = Counter([frm for (frm, _) in plan.values() if frm is not None])
        return {
            "moved_count": float(len(plan)),
            "by_to": dict(by_to),
            "by_from": dict(by_from),
        }

class CacheRebalancer:
    def __init__(self, cache: DistributedCache):
        self.cache = cache

    def execute(self, plan: Dict[str, Tuple[Optional[str], Optional[str]]], ring_before: ConsistentHasher, ttl_sec: int = 1800):
        """Warmup new owners: read from old placement and write to new placement."""
        for key, (frm, to) in plan.items():
            # Try read from any node in old placement
            old_nodes = self.cache.placement_with_ring(key, ring_before)
            val = None
            for n in old_nodes:
                if n is None: continue
                val = n.get(key)
                if val is not None:
                    break
            if val is None:
                # If not present, fall back to current cache.get (may compute upstream in real systems)
                val = self.cache.get(key)
            if val is not None:
                self.cache.set(key, val, ttl_sec=ttl_sec)

class VectorRebalancer:
    def __init__(self, router: VectorRouter):
        self.router = router

    def execute(self, moved_ids: Dict[str, Tuple[Optional[str], Optional[str]]], ring_before: ConsistentHasher):
        for vid, (frm, to) in moved_ids.items():
            # Read from any shard in old placement
            old_shards = self.router.placement_with_ring(vid, ring_before)
            vec = None
            for s in old_shards:
                if s is None: continue
                vec = s.get(vid)
                if vec is not None:
                    break
            if vec is not None:
                # Upsert into new placement via current router
                self.router.upsert(vid, vec)

class ArtifactRebalancer:
    def __init__(self, dist: ArtifactDistributor):
        self.dist = dist

    def execute(self, moved_artifacts: Dict[str, Tuple[Optional[str], Optional[str]]], ring_before: ConsistentHasher):
        for key, (frm, to) in moved_artifacts.items():
            # Read from any host in old placement
            old_hosts = self.dist.placement_with_ring(key, ring_before)
            blob = None
            for h in old_hosts:
                if h is None: continue
                blob = h.get(key)
                if blob is not None:
                    break
            if blob is not None:
                # Distribute to new placement via current dist
                self.dist.distribute(key, blob)
