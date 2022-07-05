import datetime
import json
from typing import Dict, List

from fabric import Connection
from invoke.exceptions import UnexpectedExit
import loguru
from pydantic import IPvAnyAddress, ValidationError

from .schemas import ClusterConfig, NodeOpts

logger = loguru.logger


def default_daemon_config(gpu_uuids: List[str]) -> Dict:
    daemon_config = {
        "runtimes": {"nvidia": {"path": "nvidia-container-runtime"}, "runtimeArgs": []},
        "default-runtime": "nvidia",
    }
    resource_list = []
    for uuid in gpu_uuids:
        uuid_string = f"gpu={uuid[0:12]}"
        resource_list.append(uuid_string)
    daemon_config["node-generic-resources"] = resource_list
    return daemon_config


def update_daemon_config(daemon_config: Dict, gpu_uuids: List) -> Dict:
    assert (
        "nvidia" in daemon_config["runtimes"]
    ), "nvidia not found in listed docker runtimes"
    daemon_config["default-runtime"] = "nvidia"

    resource_list = daemon_config.get("node-generic-resources", [])
    for uuid in gpu_uuids:
        uuid_string = f"gpu={uuid[0:12]}"
        if uuid_string not in resource_list:
            resource_list.append(uuid_string)
    daemon_config["node-generic-resources"] = resource_list
    return daemon_config


def ask_yes_no(question: str) -> bool:
    positive_responses = ["yes", "y"]
    negative_responses = ["no", "n"]
    done = False
    while not done:
        logger.info(question)
        logger.info('respond "yes"/"y" or "no"/"n"')
        choice = input().lower()
        if choice in positive_responses:
            return True
        elif choice in negative_responses:
            return False


def configure_node_gpu_up(node_name: str, ip_address: IPvAnyAddress) -> bool:
    logger.info(
        f"Attempting to configure {node_name} for GPU use in MPI cluster. This may"
        " ask for sudo password."
    )
    try:
        with Connection(ip_address.exploded) as conn:
            nvidia_smi_result = conn.run("nvidia-smi -a", hide=True)
            if nvidia_smi_result.failed:
                logger.error(
                    f"Failed to run nvidia-smi on {node_name}, node can't be configured"
                )
            uuid_lines = []
            for line in nvidia_smi_result.stdout.split("\n"):
                if "GPU UUID" in line:
                    uuid = line.split(":")[1].lstrip()
                    uuid_lines.append(uuid)
            if uuid_lines is []:
                logger.error(
                    f"Could not find GPU UUID in nvidia-smi output on {node_name}, node"
                    " will not be configured"
                )
                return
            check_nvidia_docker = conn.run("which nvidia-docker", hide=True)
            if check_nvidia_docker.failed:
                logger.error(
                    f"nvidia-docker not found on {node_name}. Can't configure docker to"
                    " use Nvidia GPU's without it."
                )
                return

            logger.info(
                f"Found {len(uuid_lines)} GPUs on {node_name}. Configuring docker to"
                " use these GPUs."
            )
            logger.info(
                "The next few steps will require you to enter the sudo password for"
                f" {node_name}."
            )

            daemon_file = "/etc/docker/daemon.json"
            daemon_file_bak = "/etc/docker/daemon.json-{:%Y-%m-%d-%H:%M:%S}".format(
                datetime.datetime.now()
            )
            current_daemon_file_result = conn.run(f"cat {daemon_file}", hide=True)
            if current_daemon_file_result.failed:
                logger.info(
                    f"No daemon config file found on {node_name}, creating new one"
                )
                config = default_daemon_config(uuid_lines)
            else:
                logger.info(f"Backing up current config file to {daemon_file_bak}")
                print(f"enter sudo password for {node_name}")
                cp_result = conn.run(
                    f"sudo cp {daemon_file} {daemon_file_bak}", hide=True, pty=True
                )
                if cp_result.failed:
                    logger.error(
                        "Failed to make a backup of docker daemon.json file on"
                        f" {node_name}, will not continue."
                    )
                    return

                try:
                    config = update_daemon_config(
                        daemon_config=json.loads(current_daemon_file_result.stdout),
                        gpu_uuids=uuid_lines,
                    )

                except AssertionError as err:
                    logger.error(
                        f"could not update docker daemon.json for {node_name}, got"
                        f" error: {err}"
                    )
                    return
                logger.info(
                    f"Writing new docker daemon config to {daemon_file} on {node_name}"
                )
                print(f"enter sudo password for {node_name}")
                write_daemon_json_result = conn.run(
                    f"echo '{json.dumps(config, indent=4)}' | sudo tee {daemon_file} >"
                    " /dev/null && sudo systemctl restart docker.service",
                    hide=True,
                    pty=True,
                )

                if write_daemon_json_result.failed:
                    logger.error(f"failed to write {daemon_file} on {node_name}.")
                else:
                    logger.info(
                        f"{node_name} properly configured to use GPUs in docker swarm"
                        " deployments."
                    )

    except UnexpectedExit as err:
        logger.error(
            f"Failed to connect and update docker GPU config on node {node_name}:"
            f" {ip_address}, got error: {err}"
        )


def configure_node_gpus(cluster_config: ClusterConfig) -> None:
    nodes = cluster_config.managers
    nodes.update(cluster_config.workers)

    response = ask_yes_no(
        "Configuring node GPUs will make changes to the docker configuration on these"
        " nodes. Do you want to continue?"
    )
    if response is False:
        return

    for node_name, ip_address in nodes.items():
        configure_node_gpu_up(node_name=node_name, ip_address=ip_address)


def node_control(
    cluster_config: ClusterConfig,
    configure_gpus: bool = True,
) -> None:
    try:
        node_opts = NodeOpts(gpu_config=configure_gpus)
    except ValidationError as err:
        logger.error(f"invalid cluster control configuration: {err}")
        return

    if node_opts.gpu_config:
        configure_node_gpus(cluster_config)

    return
