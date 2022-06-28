import loguru
from invoke.exceptions import UnexpectedExit
from fabric import Connection
from pydantic import ValidationError
from typing import Any, Dict

import yaml

from .schemas import ClusterConfig, ClusterOpts
from .swarm_management import registry_is_initialized, swarm_is_initialized

logger = loguru.logger


def build_and_push_image(cluster_config: ClusterConfig) -> bool:
    manager = list(cluster_config.managers.keys())[0]
    ip_address = cluster_config.managers[manager]

    try:
        with Connection(ip_address.exploded) as conn:
            try:
                conn.put(
                    local=str(cluster_config.cluster_spec.dockerfile),
                    remote=str(cluster_config.cluster_spec.nfs_root),
                )
            except FileNotFoundError as err:
                logger.error(
                    f"dockerfile: {cluster_config.cluster_spec.dockerfile} not found"
                )
                return False
            except PermissionError as err:
                logger.error(
                    "got permission error when attempting to load dockerfile to"
                    " manager - does user have access to"
                    f" {cluster_config.cluster_spec.nfs_root} on {manager}?"
                )
                return False
            dockerfile_path = (
                cluster_config.cluster_spec.nfs_root
                / cluster_config.cluster_spec.dockerfile.name
            )
            image_name = "127.0.0.1:5000/mpi"
            result = conn.run(
                f"docker build -t {image_name} -f"
                f" {dockerfile_path} {cluster_config.cluster_spec.nfs_root}",
                hide=False,
            )
            if result.failed:
                logger.error(
                    f"Failed to build and push docker image on manager {manager}:"
                    f" {ip_address}"
                )
                logger.error(f"stdout: {result.stdout}")
                logger.error(f"stderr: {result.stderr}")
                return False
            else:
                logger.info(
                    f"image: {cluster_config.cluster_spec.dockerfile} built and pushed"
                    " to cluster registry"
                )
                return True
    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and build MPI image on manager {manager}: {ip_address},"
            f" got error: {err}"
        )
    return False


def deploy_mpi_cluster(cluster_config: ClusterConfig) -> None:
    # image_status = build_and_push_image(cluster_config=cluster_config)
    # if not image_status:
    #     logger.error("Docker image couldn't be built: can't deploy MPI cluster")
    #     return

    manager = list(cluster_config.managers.keys())[0]
    ip_address = cluster_config.managers[manager]
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
                    f"Failed to deploy MPI cluster from manager {manager}: {ip_address}"
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
    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and deploy MPI cluster on manager {manager}:"
            f" {ip_address}, got error: {err}"
        )


def build_compose_config(cluster_config: ClusterConfig) -> Dict[str, Any]:
    return {
        "version": "3.8",
        "services": {
            "master": {
                "image": "127.0.0.1:5000/mpi",
                "build": f"{cluster_config.cluster_spec.build_context}",
                "deploy": {"placement": {"constraints": ["node.role==manager"]}},
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
            },
        },
    }


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
