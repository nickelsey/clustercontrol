from typing import Union

import loguru
from invoke.exceptions import UnexpectedExit
from fabric import Connection
from pydantic import IPvAnyAddress, ValidationError

from cluster_management import cluster_is_initialized
from schemas import ConnectionOpts, ClusterConfig
from swarm_management import registry_is_initialized, swarm_is_initialized

logger = loguru.logger


def connection_control(
    cluster_config: ClusterConfig, host: Union[str, IPvAnyAddress, None]
) -> None:
    try:
        connect_opts = ConnectionOpts(cluster_config=cluster_config, host=host)
    except ValidationError as err:
        logger.error(f"invalid connection option configuration: {err}")
        return

    if connect_opts.host == None:
        host = list(cluster_config.managers.keys())[0]
        service_name = "mpi_cluster_master"
    else:
        host = connect_opts.host
    ip_address, service_name = (
        cluster_config.managers[host],
        "mpi_cluster_master"
        if host in cluster_config.managers.keys()
        else cluster_config.workers[host],
        "mpi_cluster_worker",
    )

    try:
        with Connection(ip_address.exploded) as conn:
            conn.run(
                command=(
                    f"docker exec -it $(docker ps -q -f name={service_name}) bash -l"
                ),
                pty=True,
            )
    except UnexpectedExit as err:
        logger.error(f"failed to connect to {host}: {ip_address}")

    logger.info(f"Connection to {host}: {ip_address} closed.")
