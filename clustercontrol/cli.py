#!/usr/bin/env python3

import sys
from pathlib import Path

import fire
import loguru
import yaml
from pydantic import (
    BaseModel,
    ValidationError,
)
from typing import Union

from cluster_connection import connection_control
from cluster_management import cluster_control
from schemas import ClusterConfig
from swarm_management import swarm_control

logger = loguru.logger


def load_and_validate_yaml_config(
    yml_path: str, validator: BaseModel, file_descriptor: str
) -> ClusterConfig:
    yml = None
    try:
        with open(yml_path, "r") as f:
            try:
                yml = yaml.safe_load(f)
            except yaml.YAMLError as err:
                logger.error(f"failed to parse {file_descriptor} file: {err}")
    except FileNotFoundError as err:
        logger.error(f"failed to load cluster config {file_descriptor}: {err}")
    if yml:
        try:
            return validator(**yml)
        except ValidationError as err:
            logger.error(f"{file_descriptor} file doesn't match schema: {err}")
    sys.exit()


class ClusterController(object):
    """Command line interface for deploying, controlling, and tearing down a
    multi-machine MPI cluster via Docker Swarm.
    """

    def __init__(self, cluster_config: Path):
        try:
            self.cluster_config = load_and_validate_yaml_config(
                cluster_config, ClusterConfig, "cluster config"
            )
        except ValidationError as err:
            logger.error("Cluster config has logical error: {err}")
            return

    def swarm(self, up=False, down=False):
        swarm_control(cluster_config=self.cluster_config, swarm_up=up, swarm_down=down)

    def cluster(self, up=False, down=False):
        cluster_control(
            cluster_config=self.cluster_config, cluster_up=up, cluster_down=down
        )

    def connect(self, host: Union[Path, None] = None):
        connection_control(cluster_config=self.cluster_config, host=host)


if __name__ == "__main__":
    fire.Fire(ClusterController, name="cluster_controller")
