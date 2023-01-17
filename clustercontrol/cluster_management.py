import loguru
from invoke.exceptions import UnexpectedExit
from fabric import Connection
from pydantic import ValidationError
from typing import Any, Dict

import yaml
import json

from .schemas import ClusterConfig, ClusterOpts
from .swarm_management import registry_is_initialized, swarm_is_initialized

logger = loguru.logger


def deploy_mpi_cluster(cluster_config: ClusterConfig) -> None:
    manager = list(cluster_config.managers.keys())[0]
    ip_address = cluster_config.managers[manager]

    try:
        with Connection(ip_address.exploded) as conn:
            try:
                res = conn.put(
                    local=str(cluster_config.cluster_spec.dockerfile),
                    remote=str(cluster_config.cluster_spec.nfs_root),
                )
            except FileNotFoundError as err:
                logger.error(
                    f"dockerfile: {cluster_config.cluster_spec.dockerfile} not found"
                )
                return
            except PermissionError as err:
                logger.error(
                    "got permission error when attempting to load dockerfile to"
                    " manager - does user have access to"
                    f" {cluster_config.cluster_spec.nfs_root} on {manager}?"
                )
                return
    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and build MPI image on manager {manager}: {ip_address},"
            f" got error: {err}"
        )

    local_tmp_file = "compose_config_tmp_file.yml"
    compose_config = build_compose_config(cluster_config)

    with open(local_tmp_file, "w") as f:
        yaml.dump(compose_config, f, Dumper=yaml.Dumper)
    remote_file = cluster_config.cluster_spec.nfs_root / "docker-compose.yml"
    try:
        with Connection(ip_address.exploded) as conn:
            conn.put(
                local=str(local_tmp_file),
                remote=str(remote_file),
            )
            with conn.cd(cluster_config.cluster_spec.nfs_root):
                build_push_result = conn.run(
                    "docker compose build && docker compose push", hide=True
                )
                if build_push_result.failed:
                    logger.error(
                        f"failed to build and push compose image on manager {manager}:"
                        f" {ip_address}"
                    )
                    logger.error(f"config: {compose_config}")
                    logger.error(f"stdout: {build_push_result.stdout}")
                    logger.error(f"stderr: {build_push_result.stderr}")
                    return
                deploy_result = conn.run(
                    f"docker stack deploy -c {remote_file} mpi_cluster",
                    hide=True,
                )
                if deploy_result.failed:
                    logger.error(
                        f"Failed to deploy MPI cluster from manager {manager}:"
                        f" {ip_address}"
                    )
                    logger.error(f"config: {compose_config}")
                    logger.error(f"stdout: {deploy_result.stdout}")
                    logger.error(f"stderr: {deploy_result.stderr}")
                else:
                    logger.info("MPI cluster deployed with config:")
                    logger.info(compose_config)
                cleanup_result = conn.run(f"rm {remote_file}")
                if cleanup_result.failed:
                    logger.error(
                        "warning: could not delete compose file on manager"
                        f" {manager} {ip_address}: {remote_file}"
                    )
                # add a hosts.txt file to the nfs directory by inspecting docker service
                docker_services = conn.run("docker service ls", hide=True).stdout.split(
                    "\n"
                )
                service_ids = []
                for l in docker_services:
                    if "mpi_cluster_master" in l or "mpi_cluster_worker" in l:
                        service_ids.append(l.split()[0])
                cluster_ips = []
                for service_id in service_ids:
                    service_info = conn.run(f"docker inspect {service_id}", hide=True)
                    service_json = json.loads(service_info.stdout)
                    for entry in service_json:
                        ips = entry["Endpoint"]["VirtualIPs"]
                        assert len(ips) == 1, "don't handle the multi-ip case yet"
                        cluster_ips.append(ips[0]["Addr"][:-3])
                for idx, ip in enumerate(cluster_ips):
                    if idx == 0:
                        conn.run(f"echo {ip} > hosts.txt", hide=True)
                    else:
                        conn.run(f"echo {ip} >> hosts.txt", hide=True)
    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and deploy MPI cluster on manager {manager}:"
            f" {ip_address}, got error: {err}"
        )


def build_compose_config(cluster_config: ClusterConfig) -> Dict[str, Any]:
    config = {
        "version": "3.8",
        "services": {
            "master": {
                "image": "127.0.0.1:5000/mpi",
                "build": f"{cluster_config.cluster_spec.build_context}",
                "deploy": {
                    "placement": {"constraints": ["node.role==manager"]},
                },
                "volumes": [
                    f"{cluster_config.cluster_spec.nfs_root}:{cluster_config.cluster_spec.nfs_mount}"
                ],
            },
            "worker": {
                "image": "127.0.0.1:5000/mpi",
                "depends_on": ["master"],
                "deploy": {
                    "replicas": cluster_config.cluster_spec.replicas,
                    "placement": {
                        "max_replicas_per_node": cluster_config.cluster_spec.max_replicas_per_node
                    },
                },
                "volumes": [
                    f"{cluster_config.cluster_spec.nfs_root}:{cluster_config.cluster_spec.nfs_mount}"
                ],
            },
        },
    }
    return config


def teardown_mpi_cluster(cluster_config: ClusterConfig) -> None:
    manager = list(cluster_config.managers.keys())[0]
    ip_address = cluster_config.managers[manager]

    try:
        with Connection(ip_address.exploded) as conn:
            result = conn.run(f"docker stack rm mpi_cluster", hide=True)
            if result.failed:
                logger.error(
                    f"Failed to teardown MPI cluster from manager {manager}:"
                    f" {ip_address}"
                )
                logger.error(f"stdout: {result.stdout}")
                logger.error(f"stderr: {result.stderr}")
            else:
                result = conn.run(
                    f"rm {cluster_config.cluster_spec.nfs_root}/hosts.txt", hide=True
                )
                if result.failed:
                    logger.error(
                        "failed to delete hosts file from NFS mount:"
                        f" {cluster_config.cluster_spec.nfs_root}/hosts.txt. It may"
                        " have already been deleted, or there may be an issue with"
                        " tearing down the cluster."
                    )
                logger.info("MPI cluster teardown successful")
    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and teardown MPI cluster on manager {manager}:"
            f" {ip_address}, got error: {err}"
        )


def cluster_control(
    cluster_config: ClusterConfig, cluster_up: bool = False, cluster_down: bool = False
) -> None:
    try:
        cluster_opts = ClusterOpts(cluster_up=cluster_up, cluster_down=cluster_down)
    except ValidationError as err:
        logger.error(f"invalid cluster control configuration: {err}")
        return

    swarm_status = swarm_is_initialized(
        cluster_config=cluster_config
    ) and registry_is_initialized(cluster_config=cluster_config)
    if not swarm_status:
        logger.error(
            "swarm and registry must be initialized before an MPI cluster can be"
            " deployed."
        )
        return

    if cluster_opts.cluster_up:
        deploy_mpi_cluster(cluster_config=cluster_config)
    elif cluster_opts.cluster_down:
        teardown_mpi_cluster(cluster_config=cluster_config)


def cluster_is_initialized(cluster_config: ClusterConfig) -> bool:
    manager = list(cluster_config.managers.keys())[0]
    ip_address = cluster_config.managers[manager]

    try:
        with Connection(ip_address.exploded) as conn:
            result = conn.run(f"docker stack ls")
            if result.failed:
                logger.error(
                    f"Failed to teardown get cluster stack list from manager {manager}:"
                    f" {ip_address}"
                )
                logger.error(f"stdout: {result.stdout}")
                logger.error(f"stderr: {result.stderr}")
                return False
    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and check cluster status on manager {manager}:"
            f" {ip_address}, got error: {err}"
        )
        return False

    stack_list = result.stdout
    for stack in stack_list:
        if stack:
            splits = stack.split(" ")
            service_name = splits[0]
            if service_name == "mpi_cluster":
                return True

    return False
