from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.config import settings
from src.logging_utils import setup_logging


LOGGER = logging.getLogger("data_gen")


def _asset_id(i: int) -> str:
    return f"ASSET_{i:03d}"


def generate_telemetry_event(asset_id: str, ts: datetime) -> dict[str, Any]:
    temp_c = random.uniform(45, 120)
    vibration_mm_s = random.uniform(0.1, 8.5)
    pressure_kpa = random.uniform(500, 4000)
    production_rate_bbl_hr = random.uniform(10, 140)
    status = "DOWN" if random.random() < 0.06 else "RUNNING"
    return {
        "asset_id": asset_id,
        "event_ts": ts.isoformat(),
        "temperature_c": round(temp_c, 2),
        "vibration_mm_s": round(vibration_mm_s, 3),
        "pressure_kpa": round(pressure_kpa, 2),
        "production_rate_bbl_hr": round(production_rate_bbl_hr, 3),
        "status": status,
        "unit_temperature": "C",
        "unit_pressure": "kPa",
    }


def generate_maintenance_logs(base_dir: Path, asset_count: int, days: int = 7) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    wo_path = base_dir / "maintenance_work_orders.csv"
    fail_path = base_dir / "failure_events.json"
    now = datetime.now(timezone.utc)

    with wo_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "work_order_id",
                "asset_id",
                "start_ts",
                "end_ts",
                "issue_code",
                "downtime_minutes",
            ],
        )
        writer.writeheader()
        work_order_id = 1
        for i in range(1, asset_count + 1):
            for _ in range(random.randint(3, 7)):
                start_offset_days = random.randint(0, max(days - 1, 1))
                start_ts = now - timedelta(days=start_offset_days, hours=random.randint(0, 23))
                duration_min = random.randint(20, 360)
                end_ts = start_ts + timedelta(minutes=duration_min)
                writer.writerow(
                    {
                        "work_order_id": f"WO-{work_order_id:05d}",
                        "asset_id": _asset_id(i),
                        "start_ts": start_ts.isoformat(),
                        "end_ts": end_ts.isoformat(),
                        "issue_code": random.choice(["PUMP_FAIL", "VALVE", "SENSOR", "POWER", "OVERHEAT"]),
                        "downtime_minutes": duration_min,
                    }
                )
                work_order_id += 1

    failures: list[dict[str, Any]] = []
    for i in range(1, asset_count + 1):
        for _ in range(random.randint(3, 8)):
            fail_ts = now - timedelta(days=random.randint(0, max(days - 1, 1)), hours=random.randint(0, 23))
            failures.append(
                {
                    "asset_id": _asset_id(i),
                    "failure_ts": fail_ts.isoformat(),
                    "failure_type": random.choice(["MECH", "ELEC", "HYD", "CONTROL"]),
                }
            )
    with fail_path.open("w", encoding="utf-8") as f:
        json.dump(failures, f, indent=2)

    LOGGER.info("Wrote maintenance files: %s and %s", wo_path, fail_path)


def produce_stream(seconds: int | None = None) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    started = datetime.now(timezone.utc)
    while True:
        now = datetime.now(timezone.utc)
        if seconds is not None and (now - started).total_seconds() > seconds:
            break
        for i in range(1, settings.asset_count + 1):
            event = generate_telemetry_event(_asset_id(i), now)
            producer.send(settings.kafka_topic, event)
            LOGGER.info("Produced event asset_id=%s ts=%s", event["asset_id"], event["event_ts"])
        producer.flush()
        time.sleep(settings.telemetry_interval_sec)


def generate_once_local_files() -> None:
    settings.raw_data_dir.mkdir(parents=True, exist_ok=True)
    telemetry_path = settings.raw_data_dir / "telemetry_seed.jsonl"
    now = datetime.now(timezone.utc)
    with telemetry_path.open("w", encoding="utf-8") as f:
        for _ in range(100):
            asset_id = _asset_id(random.randint(1, settings.asset_count))
            event = generate_telemetry_event(asset_id, now - timedelta(seconds=random.randint(0, 86400)))
            f.write(json.dumps(event) + "\n")
    LOGGER.info("Wrote seed telemetry file: %s", telemetry_path)
    generate_maintenance_logs(settings.raw_data_dir, settings.asset_count)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic telemetry and maintenance logs.")
    parser.add_argument("--mode", choices=["once", "stream"], default="once")
    parser.add_argument("--seconds", type=int, default=None, help="For stream mode, optional max runtime.")
    args = parser.parse_args()

    setup_logging(settings.log_level)
    if args.mode == "once":
        generate_once_local_files()
    else:
        produce_stream(seconds=args.seconds)


if __name__ == "__main__":
    main()
