from __future__ import annotations

import logging

from data_gen.generate_data import generate_once_local_files
from dq.run_checks import main as run_dq
from pipelines.batch.build_silver_from_seed import main as build_silver_local
from pipelines.batch.compute_gold import main as compute_gold
from pipelines.batch.load_gold_to_postgres import main as load_gold
from src.config import settings
from src.logging_utils import setup_logging


LOGGER = logging.getLogger("bootstrap_deploy")


def main() -> None:
    setup_logging(settings.log_level)
    LOGGER.info("Starting deploy bootstrap workflow.")

    generate_once_local_files()
    build_silver_local()
    compute_gold()
    run_dq()
    load_gold()

    LOGGER.info("Deploy bootstrap workflow completed successfully.")


if __name__ == "__main__":
    main()
