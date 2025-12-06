# AI Consistent Hashing (xxhash) — Exercise 2

This repository contains implementation of a consistent hashing ring built on xxh3_64.
It includes virtual nodes, optional weights, replicas, and adapters for:

- Distributed cache for model responses/embeddings
- Vector DB / feature store sharding
- Model artifact distribution (checkpoints/LoRA)


## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python tests_consistent_hasher.py
python demo.py
```

## Rebalance Plan (Manual, No Hooks)

The plan is implemented in `rebalance.py` and works in three steps:

1. **Plan**: Compare owners before vs after membership change → list of moved keys (primary owner changes only).
2. **Warmup/Backfill**:
   - Cache: read value from old placement; write to new placement (replicas).
   - Vector: read vector from old shard; upsert into new shard.
   - Artifacts: read blob from old host; put into new host(s).
3. **Report**: Print moved fraction and per-node counts.

### Usage (example)

```python
from consistent_hash_ring import ConsistentHasher
from cache_cluster import DistributedCache, InMemoryCacheNode
from vector_shard_router import VectorRouter, VectorShard
from artifact_placement import ArtifactDistributor, ArtifactHost
from rebalance import RebalancePlanner, CacheRebalancer, VectorRebalancer, ArtifactRebalancer

# Build services and attach nodes (as in demo.py)...
# Take a snapshot of the ring BEFORE change
ring_before = cache.ring.clone()

# ADD a node (or REMOVE) on the live adapters (this mutates their ring)
cache.attach_node("cache-d", InMemoryCacheNode("cache-d", capacity_items=5000), weight=1)
ring_after = cache.ring

# Prepare a representative key list (e.g., last 10k signatures)
keys = [f"key-{i}" for i in range(10000)]

planner = RebalancePlanner()
plan = planner.plan_moved(keys, ring_before, ring_after)
print(planner.stats(plan))

# Execute warmup/backfill
CacheRebalancer(cache).execute(plan, ring_before)
```

> You can do the same for vector IDs and artifact keys using `VectorRebalancer` and `ArtifactRebalancer`.

## Design Notes

- Hash space: unsigned 64-bit from xxhash (xxh3_64)
- Ring: sorted tokens; O(log N) lookups via bisect
- Virtual nodes: 128–256 per unit weight to smooth distribution
- Replicas: clockwise successors; optional multiprobe to reduce hotspots
- Elasticity: small key movement on join/leave; warmup/backfill handled by `rebalance.py`

## Files
- `consistent_hash_ring.py` — core ring (clone() included for snapshotting)
- `cache_cluster.py` — cache adapter; exposes `placement_with_ring()` and node getter
- `vector_shard_router.py` — vector adapter; exposes `placement_with_ring()` and `get()`
- `artifact_placement.py` — artifact adapter; exposes `placement_with_ring()`
- `rebalance.py` — plan + executors for cache/vector/artifacts (manual, no hooks)
- `tests_consistent_hasher.py` — minimal tests
- `demo.py` — end-to-end demo wiring
