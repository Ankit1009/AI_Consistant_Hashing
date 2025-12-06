
"""Consistent hashing ring using xxh3_64.
- Sorted token array for O(log N) lookups via bisect
- Virtual nodes (weighted) per physical node
- Replica selection + multiprobe for balance
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict
import bisect
import logging
import threading
import copy

try:
    import xxhash
except ImportError as e:
    raise RuntimeError("xxhash is required. Install with: pip install xxhash") from e

log = logging.getLogger(__name__)

def h64(data: bytes, seed: int = 0) -> int:
    return xxhash.xxh3_64_intdigest(data, seed=seed)

def key_hash(key: str, seed: int = 0) -> int:
    return h64(key.encode("utf-8"), seed)



class ConsistentHasher:
    """Consistent hashing ring with virtual nodes and replica selection."""
    def __init__(self, virtual_nodes_per_weight: int = 128, seed: int = 0):
        self._seed = seed
        self._vn_per_weight = max(1, virtual_nodes_per_weight)
        self._lock = threading.RLock()
        self._ring: List[Tuple[int, Node]] = []
        self._sorted_tokens: List[int] = []
        self._nodes: Dict[str, Node] = {}

    def _vn_count(self, node: Node) -> int:
        return self._vn_per_weight * max(1, node.weight)

    def _token_for_vn(self, node_id: str, replica_idx: int) -> int:
        return h64(f"{node_id}#{replica_idx}".encode("utf-8"), seed=self._seed)

    def _insert_token(self, token: int, node: Node) -> None:
        idx = bisect.bisect_left(self._sorted_tokens, token)
        self._sorted_tokens.insert(idx, token)
        self._ring.insert(idx, (token, node))

    def add_node(self, node_id: str, weight: int = 1, zone: Optional[str] = None) -> None:
        node = Node(id=node_id, weight=weight, zone=zone)
        with self._lock:
            if node.id in self._nodes:
                raise ValueError(f"Node {node.id} already exists")
            self._nodes[node.id] = node
            vn = self._vn_count(node)
            for i in range(vn):
                token = self._token_for_vn(node.id, i)
                self._insert_token(token, node)
        log.debug("added node=%s vnodes=%d", node_id, vn)

    def remove_node(self, node_id: str) -> None:
        with self._lock:
            node = self._nodes.pop(node_id, None)
            if node is None:
                return
            new_ring = [(t, n) for (t, n) in self._ring if n.id != node_id]
            self._ring = sorted(new_ring, key=lambda x: x[0])
            self._sorted_tokens = [t for (t, _) in self._ring]
        log.debug("removed node=%s", node_id)

    def get_node(self, key: str) -> Optional[str]:
        with self._lock:
            if not self._ring:
                return None
            tok = key_hash(key, self._seed)
            idx = bisect.bisect(self._sorted_tokens, tok) % len(self._sorted_tokens)
            return self._ring[idx][1].id

    def get_nodes_for_key(self, key: str, replicas: int = 1, multiprobe: int = 1) -> List[str]:
        with self._lock:
            if not self._ring:
                return []
            n = len(self._sorted_tokens)
            starts: List[int] = []
            for p in range(multiprobe):
                tok = key_hash(f"{key}|{p}", self._seed)
                starts.append(bisect.bisect(self._sorted_tokens, tok) % n)
            starts.sort()
            out: List[str] = []
            seen: set[str] = set()
            for s in starts:
                i = s
                while len(out) < replicas and n > 0:
                    nid = self._ring[i % n][1].id
                    if nid not in seen:
                        out.append(nid)
                        seen.add(nid)
                    i += 1
                if len(out) >= replicas:
                    break
            return out

    def nodes(self) -> List[str]:
        with self._lock:
            return list(self._nodes.keys())

    def size(self) -> int:
        with self._lock:
            return len(self._nodes)

    def dump_tokens(self) -> List[Tuple[int, str]]:
        with self._lock:
            return [(t, n.id) for (t, n) in self._ring]

    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {"nodes": len(self._nodes), "tokens": len(self._ring), "vn_per_weight": self._vn_per_weight}

    def clone(self) -> "ConsistentHasher":
        """Deep clone the ring for before/after comparison """
        return copy.deepcopy(self)
@dataclass(frozen=True)
class Node:
    id: str
    weight: int = 1
    zone: Optional[str] = None
    labels: Optional[Dict[str, str]] = None