
"""Artifact distribution using the consistent ring.
Hosts store local files in a demo; swap with S3/GCS in production.
No hooks: rebalance via rebalance.py.
"""
from __future__ import annotations

from typing import List, Dict, Optional
import os
from consistent_hash_ring import ConsistentHasher

class ArtifactHost:
    def __init__(self, host_id: str, base_dir: str):
        self.host_id = host_id
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def path_for(self, artifact_key: str) -> str:
        safe = artifact_key.replace(":", "_")
        return os.path.join(self.base_dir, f"{safe}.bin")

    def has(self, artifact_key: str) -> bool:
        return os.path.exists(self.path_for(artifact_key))

    def put(self, artifact_key: str, data: bytes):
        with open(self.path_for(artifact_key), "wb") as f:
            f.write(data)

    def get(self, artifact_key: str) -> Optional[bytes]:
        p = self.path_for(artifact_key)
        if not os.path.exists(p):
            return None
        with open(p, "rb") as f:
            return f.read()

class ArtifactDistributor:
    def __init__(self, ring: ConsistentHasher, replication: int = 2):
        self.ring = ring
        self.replication = replication
        self._hosts: Dict[str, ArtifactHost] = {}

    def attach_host(self, host_id: str, host: ArtifactHost, weight: int = 1):
        self._hosts[host_id] = host
        self.ring.add_node(host_id, weight=weight)

    def detach_host(self, host_id: str):
        self._hosts.pop(host_id, None)
        self.ring.remove_node(host_id)

    def placement(self, artifact_key: str) -> List[ArtifactHost]:
        node_ids = self.ring.get_nodes_for_key(artifact_key, replicas=self.replication, multiprobe=3)
        return [self._hosts[nid] for nid in node_ids if nid in self._hosts]

    def placement_with_ring(self, artifact_key: str, ring: ConsistentHasher) -> List[ArtifactHost]:
        node_ids = ring.get_nodes_for_key(artifact_key, replicas=self.replication, multiprobe=3)
        return [self._hosts.get(nid) for nid in node_ids if nid in self._hosts]
