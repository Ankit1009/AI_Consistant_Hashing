from consistent_hash_ring import ConsistentHasher
from cache_cluster import DistributedCache, InMemoryCacheNode
from vector_shard_router import VectorRouter, VectorShard
from rebalance import RebalancePlanner, CacheRebalancer, VectorRebalancer, ArtifactRebalancer
import hashlib, json

# 1) Cache cluster
cache_ring = ConsistentHasher(virtual_nodes_per_weight=256, seed=2025)
cache = DistributedCache(cache_ring, replication=2)
for nid in ['cache-a','cache-b','cache-c']:
    cache.attach_node(nid, InMemoryCacheNode(nid, capacity_items=5000))

def request_signature(model_name: str, params: dict, prompt: str) -> str:
    payload = json.dumps({'m': model_name, 'p': params, 'q': prompt}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()

# Prepare sample keys
keys = [request_signature('llama3.1-8b', {'temp':0.2}, f'Q-{i}') for i in range(200)]

# Snapshot ring BEFORE adding new node
ring_before = cache.ring.clone()

# Add new node
cache.attach_node('cache-d', InMemoryCacheNode('cache-d', capacity_items=5000), weight=1)
ring_after = cache.ring

planner = RebalancePlanner()
plan = planner.plan_moved(keys, ring_before, ring_after)
print('Cache moved stats:', planner.stats(plan))
CacheRebalancer(cache).execute(plan, ring_before)

# 2) Vector shards
vec_ring = ConsistentHasher(virtual_nodes_per_weight=128, seed=7)
router = VectorRouter(vec_ring, replication=1)
for nid in ['vs-1','vs-2','vs-3']:
    router.attach_shard(nid, VectorShard(nid))
router.upsert('vec-42', [0.1, 0.0, 0.9])

# Snapshot BEFORE add
ring_before_vec = router.ring.clone()
router.attach_shard('vs-4', VectorShard('vs-4'), weight=1)
ring_after_vec = router.ring

ids = [f'vec-{i}' for i in range(200)]
vec_plan = planner.plan_moved(ids, ring_before_vec, ring_after_vec)
print('Vector moved stats:', planner.stats(vec_plan))
VectorRebalancer(router).execute(vec_plan, ring_before_vec)

