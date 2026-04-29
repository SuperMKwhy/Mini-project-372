import json
import logging
import pyodbc
import os
from datetime import datetime, timezone

import azure.functions as func

app = func.FunctionApp()

SQL_CONNECTION_STRING = os.environ["SqlConnectionString"]

CREATE_TABLE_IF_NOT_EXISTS = """
IF NOT EXISTS (
    SELECT * FROM sysobjects WHERE name='test_device_telemetry' AND xtype='U'
)
CREATE TABLE test_device_telemetry (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    device_id   NVARCHAR(100)   NOT NULL,
    temperature FLOAT           NOT NULL,
    humidity    FLOAT           NOT NULL,
    received_at DATETIME2       NOT NULL
)
"""

INSERT_SQL = """
INSERT INTO test_device_telemetry (device_id, temperature, humidity, received_at)
VALUES (?, ?, ?, ?)
"""


def get_connection() -> pyodbc.Connection:
    return pyodbc.connect(SQL_CONNECTION_STRING, timeout=30)


@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="%IoTHubEventHubName%",
    connection="IoTHubEventHubConnectionString",
    cardinality="ONE",
    consumer_group="$Default",
)
def iot_hub_to_sql(event: func.EventHubEvent) -> None:
    body = event.get_body().decode("utf-8")
    logging.info("Received event body: %s", body)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        logging.error("Failed to parse JSON payload: %s", exc)
        return

    device_id   = payload.get("device_id")
    temperature = payload.get("temperature")
    humidity    = payload.get("humidity")

    if any(v is None for v in [device_id, temperature, humidity]):
        logging.error(
            "Missing required fields. Got: device_id=%s, temperature=%s, humidity=%s",
            device_id, temperature, humidity,
        )
        return

    received_at = datetime.now(timezone.utc)

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(CREATE_TABLE_IF_NOT_EXISTS)
            cursor.execute(INSERT_SQL, device_id, float(temperature), float(humidity), received_at)
            conn.commit()
        logging.info("Inserted row: device_id=%s at %s", device_id, received_at)
    except pyodbc.Error as exc:
        logging.error("SQL error: %s", exc)
        raise
