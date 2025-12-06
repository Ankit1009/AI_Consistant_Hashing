from consistent_hash_ring import ConsistentHasher

# Test 1: one node
ring = ConsistentHasher(virtual_nodes_per_weight=128, seed=42)
ring.add_node('node-A')
assert ring.get_node('user-1') == 'node-A'
assert ring.get_node('embedding-123') == 'node-A'
assert ring.get_node('artifact:lora:en:1') == 'node-A'

# Test 2: spread across multiple nodes
ring.add_node('node-B')
ring.add_node('node-C')
targets = {ring.get_node(k) for k in ['k1','k2','k3','k4','k5','k6']}
assert targets.issubset({'node-A','node-B','node-C'}) and len(targets) >= 2

# Test 3: elasticity on join
keys = [f'key-{i}' for i in range(1000)]
before = [ring.get_node(k) for k in keys]
ring.add_node('node-D')
after = [ring.get_node(k) for k in keys]

moved = sum(1 for b, a in zip(before, after) if b != a)
print(f'Moved fraction after adding node-D: {moved/len(keys):.2%}')

# Test 4: replicas
replicas = ring.get_nodes_for_key('embedding-999', replicas=2, multiprobe=2)
print('Replica placement:', replicas)
assert len(replicas) == 2 and len(set(replicas)) == 2

print('All tests passed.')
