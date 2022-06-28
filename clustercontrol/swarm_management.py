from pydantic import IPvAnyAddress, ValidationError
from typing import Dict, Union


import loguru
from fabric import Connection
from invoke.exceptions import UnexpectedExit

from .schemas import ClusterConfig, SwarmOpts

logger = loguru.logger


def up_manager(name: str, ip_address: IPvAnyAddress) -> Union[str, None]:
    logger.info(f"Initializing swarm manager on {name}: {ip_address}")
    try:
        with Connection(ip_address.exploded) as conn:
            result = conn.run(
                f"docker swarm init --advertise-addr {ip_address}", hide=True
            )
    except UnexpectedExit as err:
        logger.error(
            f"Could not initialize manager and registry on {name}: {ip_address}. Got"
            f" error: {err}",
            hide=True,
        )
        return None
    for line in result.stdout.split("\n"):
        if "docker swarm join" in line:
            return line.lstrip()
    logger.error("Did not find proper join command for workers in manager output")
    return None


def up_registry(name: str, ip_address: IPvAnyAddress) -> bool:
    logger.info(f"Initializing registry on manager node {name}: {ip_address}")
    try:
        with Connection(ip_address.exploded) as conn:
            result = conn.run(
                "docker service create --name registry --publish"
                " published=5000,target=5000 registry:2",
                hide=True,
            )
            return True
    except UnexpectedExit as err:
        logger.error(
            f"Failed to init registry on swarm manager node {name}: {ip_address}, got"
            f" error {err}. Tearing down manager."
        )
        down_manager(name, ip_address)
        return False


def up_managers(managers: Dict[str, IPvAnyAddress]) -> Union[str, None]:
    result = None
    for name, address in managers.items():
        result = up_manager(name, address)
        if result == None:
            return None

    node = list(managers.keys())[0]
    ip_address = managers[node]
    registry_result = up_registry(node, ip_address)
    return result if result and registry_result else None


def down_manager(name: str, ip_address: IPvAnyAddress) -> bool:
    logger.info(f"Removing swarm manager on {name}: {ip_address}")
    try:
        with Connection(ip_address.exploded) as conn:
            result = conn.run("docker swarm leave --force", hide=True)
            return True
    except UnexpectedExit as err:
        logger.error(
            f"failed to remove swarm manager on {name}: {ip_address}, got error: {err}"
        )
        return False


def down_managers(managers: Dict[str, IPvAnyAddress]) -> bool:
    state = []
    for name, address in managers.items():
        result = down_manager(name, address)
        state.append(result)
    return all(state)


def up_workers(workers: Dict[str, IPvAnyAddress], join_command: str) -> bool:
    state = []
    for name, ip_address in workers.items():
        logger.info(f"adding worker to swarm on {name}: {ip_address}")
        try:
            with Connection(ip_address.exploded) as conn:
                result = conn.run(join_command, hide=True)
                state.append(True)
        except UnexpectedExit as err:
            logger.error(
                f"failed to add worker {name}: {ip_address} to swarm, got error: {err}"
            )
            state.append(False)
    return all(state)


def down_workers(workers: Dict[str, IPvAnyAddress]) -> bool:
    state = []
    for name, ip_address in workers.items():
        logger.info(f"removing worker from swarm on {name}: {ip_address}")
        try:
            with Connection(ip_address.exploded) as conn:
                result = conn.run("docker swarm leave", hide=True)
            state.append(True)
        except UnexpectedExit as err:
            logger.error(
                f"failed to remove worker {name}: {ip_address} from swarm, got error:"
                f" {err}"
            )
            state.append(False)
    return all(state)


def swarm_control(
    cluster_config: ClusterConfig,
    swarm_up: bool = False,
    swarm_down: bool = False,
) -> None:
    try:
        swarm_opts = SwarmOpts(swarm_up=swarm_up, swarm_down=swarm_down)
    except ValidationError as err:
        logger.error(f"invalid swarm option configuration: {err}")
        return
    if swarm_opts.swarm_up:
        logger.info("bringing up swarm")
        manager_result = up_managers(cluster_config.managers)
        if not manager_result:
            logger.error("could not initialize swarm")
            return
        workers_status = up_workers(cluster_config.workers, manager_result)
        if not workers_status:
            logger.error(
                "Could not initialize all workers. Make sure all workers can be"
                " accessed via passwordless SSH and have docker configured."
            )
            return
        else:
            logger.info("swarm initialized successfully")
    elif swarm_opts.swarm_down:
        logger.info("bringing down swarm")
        workers_status = down_workers(cluster_config.workers)
        manager_status = down_managers(cluster_config.managers)
        if not workers_status or not manager_status:
            logger.error(
                "Could not bring down every listed node. The swarm should be manually"
                " cleaned up."
            )
        else:
            logger.info("swarm cleaned up successfully")


def swarm_is_initialized(cluster_config: ClusterConfig) -> bool:
    config_nodes = set(cluster_config.managers.keys()) | set(
        cluster_config.workers.keys()
    )
    manager = list(cluster_config.managers.keys())[0]
    ip_address = cluster_config.managers[manager]
    try:
        with Connection(ip_address.exploded) as conn:
            result = conn.run("docker node ls", hide=True)
            if result.failed:
                logger.error(
                    f"failed to get node list on manager {manager}: {ip_address}"
                )
                logger.error(f"stdout: {result.stdout}")
                logger.error(f"stderr: {result.stderr}")
                return False
    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and check swarm status on manager {manager}:"
            f" {ip_address}, got error: {err}"
        )
        return False

    swarm_node_list = result.stdout.split("\n")[1:]

    swarm_nodes = set()
    for node_listing in swarm_node_list:
        if node_listing:
            splits = node_listing.split(" ")
            node_name = splits[4] or splits[5]
            swarm_nodes.add(node_name)

    if swarm_nodes != config_nodes:
        return False

    return True


def registry_is_initialized(cluster_config: ClusterConfig) -> bool:
    manager = list(cluster_config.managers.keys())[0]
    ip_address = cluster_config.managers[manager]

    try:
        with Connection(ip_address.exploded) as conn:
            result = conn.run("docker service ls", hide=True)
            if result.failed:
                logger.error(
                    f"failed to get service list on manager {manager}: {ip_address}"
                )
                logger.error(f"stdout: {result.stdout}")
                logger.error(f"stderr: {result.stderr}")
                return False
    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and check registry status on manager {manager}:"
            f" {ip_address}, got error: {err}"
        )
        return False

    service_list = result.stdout.split("\n")[1:]
    for service in service_list:
        if service:
            splits = service.split(" ")
            service_name = splits[3]
            if service_name == "registry":
                return True

    return False
