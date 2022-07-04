import socket
from pathlib import Path

from pydantic import BaseModel, IPvAnyAddress, root_validator, validator
from typing import Dict, Optional, Union


class ClusterSpec(BaseModel):
    nfs_root: Path
    nfs_mount: Path
    build_context: Optional[Path]
    dockerfile: Path
    replicas: int
    max_replicas_per_node: int

    @validator("dockerfile")
    def check_for_dockerfile(cls, v):
        assert v.name == "Dockerfile", "Dockerfile name must be exactly Dockerfile"
        return v

    @validator("nfs_root", "nfs_mount")
    def check_for_absolute_path(cls, v):
        assert v.is_absolute, f"must be absolute path"
        return v

    @root_validator
    def set_programatic_context(cls, v):
        if v.get("build_context") is None:
            print("setting")
            v["build_context"] = v.get("nfs_root")
        return v


class ClusterConfig(BaseModel):
    managers: Dict[str, IPvAnyAddress]
    workers: Optional[Dict[str, IPvAnyAddress]]
    cluster_spec: ClusterSpec

    @validator("managers")
    def check_manager_length(cls, v):
        assert len(v) == 1, "Currently only supports exactly 1 manager"
        return v

    @root_validator
    def check_cluster_size(cls, v):
        cluster_spec = v.get("cluster_spec")
        n_hosts = len(v.get("workers"))
        replicas = cluster_spec.replicas
        replicas_per_node = cluster_spec.max_replicas_per_node

        assert n_hosts * replicas_per_node >= replicas, (
            f"number of worker nodes not sufficient to hold {replicas} replicas, given"
            f" the limit of {replicas_per_node} replicas per node"
        )
        return v


class SwarmOpts(BaseModel):
    swarm_up: bool
    swarm_down: bool

    @root_validator
    def only_one_opt(cls, v):
        swarm_up, swarm_down = v.get("swarm_up"), v.get("swarm_down")
        assert (
            swarm_up != swarm_down
        ), "One and only one of swarm_up or swarm_down must be true"
        return v


class ClusterOpts(BaseModel):
    cluster_up: bool
    cluster_down: bool

    @root_validator
    def only_one_opt(cls, v):
        cluster_up, cluster_down = v.get("cluster_up"), v.get("cluster_down")
        assert (
            cluster_up != cluster_down
        ), "One and only one of cluster_up or cluster_down must be true"
        return v


class ConnectionOpts(BaseModel):
    cluster_config: ClusterConfig
    host: Union[str, IPvAnyAddress]

    @root_validator
    def valid_host(cls, v):
        cluster_config, host = v.get("cluster_config"), v.get("host")
        valid_hosts = (
            [None]
            + list(cluster_config.managers.keys())
            + list(cluster_config.workers.keys())
        )
        assert host in valid_hosts, f"host not recognized, must be one of {valid_hosts}"
        return v
