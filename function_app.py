import json
import logging
import pymssql
import os
from datetime import datetime, timezone

import azure.functions as func

app = func.FunctionApp()

SQL_SERVER   = os.environ["SqlServer"]
SQL_DATABASE = os.environ["SqlDatabase"]
SQL_USER     = os.environ["SqlUser"]
SQL_PASSWORD = os.environ["SqlPassword"]

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='test_device_telemetry' AND xtype='U')
    CREATE TABLE test_device_telemetry (
        id          INT IDENTITY(1,1) PRIMARY KEY,
        device_id   NVARCHAR(100)   NOT NULL,
        temperature FLOAT           NOT NULL,
        humidity    FLOAT           NOT NULL,
        received_at DATETIME2       NOT NULL
    )
    """,
    """
    IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'order_seq')
        EXEC('CREATE SEQUENCE order_seq START WITH 1 INCREMENT BY 1')
    """,
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='S2_LOGS' AND xtype='U')
    CREATE TABLE S2_LOGS (
        order_id          NVARCHAR(100)  NOT NULL PRIMARY KEY,
        arrival_time      DATETIME2      NULL,
        tare_weight_kg    FLOAT          NULL,
        departure_time    DATETIME2      NULL
    )
    """,
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='S3A_STATIC_LOGS' AND xtype='U')
    CREATE TABLE S3A_STATIC_LOGS (
        order_id              NVARCHAR(100)  NOT NULL PRIMARY KEY,
        bay_number            INT            NULL,
        arrival_time          DATETIME2      NULL,
        start_filling_time    DATETIME2      NULL,
        target_volume_liters  FLOAT          NULL,
        actual_volume_liters  FLOAT          NULL,
        final_filling_mass_kg FLOAT          NULL,
        end_filling_time      DATETIME2      NULL,
        departure_time        DATETIME2      NULL
    )
    """,
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='S3B_STATIC_LOGS' AND xtype='U')
    CREATE TABLE S3B_STATIC_LOGS (
        order_id              NVARCHAR(100)  NOT NULL PRIMARY KEY,
        bay_number            INT            NULL,
        arrival_time          DATETIME2      NULL,
        start_filling_time    DATETIME2      NULL,
        target_volume_liters  FLOAT          NULL,
        actual_volume_liters  FLOAT          NULL,
        final_filling_mass_kg FLOAT          NULL,
        end_filling_time      DATETIME2      NULL,
        departure_time        DATETIME2      NULL
    )
    """,
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='S4_LOGS' AND xtype='U')
    CREATE TABLE S4_LOGS (
        order_id          NVARCHAR(100)  NOT NULL PRIMARY KEY,
        arrival_time      DATETIME2      NULL,
        gross_weight_kg   FLOAT          NULL,
        net_weight_kg     FLOAT          NULL,
        departure_time    DATETIME2      NULL
    )
    """,
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EXIT_LOGS' AND xtype='U')
    CREATE TABLE EXIT_LOGS (
        order_id        NVARCHAR(100)  NOT NULL PRIMARY KEY,
        departure_time  DATETIME2      NULL
    )
    """,
]

# ---------------------------------------------------------------------------
# INSERT / UPDATE statements
# ---------------------------------------------------------------------------

INSERT_TEST_DEVICE = """
INSERT INTO test_device_telemetry (device_id, temperature, humidity, received_at)
VALUES (%s, %s, %s, %s)
"""

INSERT_S2 = """
INSERT INTO S2_LOGS (order_id, arrival_time, tare_weight_kg, departure_time)
VALUES (%s, %s, %s, %s)
"""

INSERT_S3A = """
INSERT INTO S3A_STATIC_LOGS
  (order_id, bay_number, arrival_time, start_filling_time,
   target_volume_liters, actual_volume_liters, final_filling_mass_kg,
   end_filling_time, departure_time)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_S3B = """
INSERT INTO S3B_STATIC_LOGS
  (order_id, bay_number, arrival_time, start_filling_time,
   target_volume_liters, actual_volume_liters, final_filling_mass_kg,
   end_filling_time, departure_time)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_S4 = """
INSERT INTO S4_LOGS (order_id, arrival_time, gross_weight_kg, net_weight_kg, departure_time)
VALUES (%s, %s, %s, %s, %s)
"""

INSERT_EXIT = """
INSERT INTO EXIT_LOGS (order_id, departure_time)
VALUES (%s, %s)
"""


# ---------------------------------------------------------------------------
# Tag-name suffixes  (last segment of the dot-separated tag id)
# ---------------------------------------------------------------------------

TAG_ORDER_ID = "OrderId"                    # PLACEHOLDER – PO number string

# S2
TAG_S2_WEIGHT    = "WT_001"
TAG_S2_ARRIVAL   = "EntryTime_S2"
TAG_S2_DEPARTURE = "ExitTime_S2"

# S3A                                       # all PLACEHOLDER – confirm tag names from PLC
TAG_S3A_BAY        = "BayNumber_S3A"
TAG_S3A_ARRIVAL    = "EntryTime_S3A"
TAG_S3A_START_FILL = "StartFillingTime_S3A"
TAG_S3A_TARGET_VOL = "TargetVolume_S3A"
TAG_S3A_ACTUAL_VOL = "ActualVolume_S3A"
TAG_S3A_FINAL_MASS = "FinalFillingMass_S3A"
TAG_S3A_END_FILL   = "EndFillingTime_S3A"
TAG_S3A_DEPARTURE  = "ExitTime_S3A"

# S3B
TAG_S3B_BAY        = "BayNumber_S3B"
TAG_S3B_ARRIVAL    = "EntryTime_S3B"
TAG_S3B_START_FILL = "StartFillingTime_S3B"
TAG_S3B_TARGET_VOL = "TargetVolume_S3B"
TAG_S3B_ACTUAL_VOL = "ActualVolume_S3B"
TAG_S3B_FINAL_MASS = "FinalFillingMass_S3B"
TAG_S3B_END_FILL   = "EndFillingTime_S3B"
TAG_S3B_DEPARTURE  = "ExitTime_S3B"

# S4
TAG_S4_GROSS     = "WeightsensorS4"
TAG_S4_NET       = "NetWeight_S4"           # PLACEHOLDER
TAG_S4_ARRIVAL   = "EntryTime_S4"           # PLACEHOLDER
TAG_S4_DEPARTURE = "ExitTime_S4"            # PLACEHOLDER

# EXIT
TAG_EXIT_DEPARTURE = "ExitTime_Exit"        # PLACEHOLDER

# ---------------------------------------------------------------------------
# Device-ID → station
# ---------------------------------------------------------------------------

DEVICE_STATION_MAP = {
    "S2_plc":   "S2",
    "S4_plc":   "S4",
    "S3A_plc":  "S3A",   # PLACEHOLDER – confirm ConnectionDeviceId
    "S3B_plc":  "S3B",   # PLACEHOLDER – confirm ConnectionDeviceId
    "EXIT_plc": "EXIT",  # PLACEHOLDER – confirm ConnectionDeviceId
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_connection() -> pymssql.Connection:
    return pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=SQL_DATABASE,
    )


def _next_order_id(cursor) -> str:
    cursor.execute("SELECT NEXT VALUE FOR order_seq")
    n = cursor.fetchone()[0]
    return f"S{n:04d}"


def _unix_sec_to_dt(v) -> datetime | None:
    if not v:
        return None
    return datetime.fromtimestamp(float(v), tz=timezone.utc)


def _to_events(payload) -> list:
    """Normalise payload to a list of event dicts regardless of whether it arrived as a single dict or an array."""
    return [payload] if isinstance(payload, dict) else payload


def _extract_tags(payload) -> dict:
    tags: dict = {}
    for event in _to_events(payload):
        for item in event.get("values", []):
            suffix = item.get("id", "").split(".")[-1]
            value  = item.get("v")
            if suffix not in tags and value:
                tags[suffix] = value
    return tags


def _get_connection_device_id(payload) -> str:
    for event in _to_events(payload):
        device_id = event.get("IoTHub", {}).get("ConnectionDeviceId")
        if device_id:
            return device_id
    return ""


def _is_plc_payload(payload) -> bool:
    if isinstance(payload, dict):
        return "values" in payload
    if isinstance(payload, list) and payload:
        return "values" in payload[0]
    return False


# ---------------------------------------------------------------------------
# Per-station inserts
# ---------------------------------------------------------------------------


def _handle_test_device(cursor, payload: dict) -> None:
    device_id   = payload.get("device_id")
    temperature = payload.get("temperature")
    humidity    = payload.get("humidity")

    if any(v is None for v in [device_id, temperature, humidity]):
        logging.error("Missing required fields. Got: device_id=%s, temperature=%s, humidity=%s",
                      device_id, temperature, humidity)
        return

    received_at = datetime.now(timezone.utc)
    cursor.execute(INSERT_TEST_DEVICE, (device_id, float(temperature), float(humidity), received_at))
    logging.info("Inserted test_device_telemetry: device_id=%s", device_id)


def _insert_s2(cursor, tags: dict) -> None:
    order_id       = tags.get(TAG_ORDER_ID) or _next_order_id(cursor)
    arrival_time   = _unix_sec_to_dt(tags.get(TAG_S2_ARRIVAL))
    departure_time = _unix_sec_to_dt(tags.get(TAG_S2_DEPARTURE))
    tare_weight    = tags.get(TAG_S2_WEIGHT)

    cursor.execute(INSERT_S2, (
        str(order_id),
        arrival_time,
        float(tare_weight) if tare_weight is not None else None,
        departure_time,
    ))
    logging.info("S2 inserted: order_id=%s arrival=%s weight=%s departure=%s", order_id, arrival_time, tare_weight, departure_time)


def _insert_s3a(cursor, tags: dict) -> None:
    order_id           = tags.get(TAG_ORDER_ID) or _next_order_id(cursor)
    bay_number         = tags.get(TAG_S3A_BAY)
    arrival_time       = _unix_sec_to_dt(tags.get(TAG_S3A_ARRIVAL))
    start_filling_time = _unix_sec_to_dt(tags.get(TAG_S3A_START_FILL))
    target_volume      = tags.get(TAG_S3A_TARGET_VOL)
    actual_volume      = tags.get(TAG_S3A_ACTUAL_VOL)
    final_mass         = tags.get(TAG_S3A_FINAL_MASS)
    end_filling_time   = _unix_sec_to_dt(tags.get(TAG_S3A_END_FILL))
    departure_time     = _unix_sec_to_dt(tags.get(TAG_S3A_DEPARTURE))

    cursor.execute(INSERT_S3A, (
        str(order_id),
        int(bay_number) if bay_number is not None else None,
        arrival_time, start_filling_time,
        float(target_volume) if target_volume is not None else None,
        float(actual_volume) if actual_volume is not None else None,
        float(final_mass) if final_mass is not None else None,
        end_filling_time, departure_time,
    ))


def _insert_s3b(cursor, tags: dict) -> None:
    order_id           = tags.get(TAG_ORDER_ID) or _next_order_id(cursor)
    bay_number         = tags.get(TAG_S3B_BAY)
    arrival_time       = _unix_sec_to_dt(tags.get(TAG_S3B_ARRIVAL))
    start_filling_time = _unix_sec_to_dt(tags.get(TAG_S3B_START_FILL))
    target_volume      = tags.get(TAG_S3B_TARGET_VOL)
    actual_volume      = tags.get(TAG_S3B_ACTUAL_VOL)
    final_mass         = tags.get(TAG_S3B_FINAL_MASS)
    end_filling_time   = _unix_sec_to_dt(tags.get(TAG_S3B_END_FILL))
    departure_time     = _unix_sec_to_dt(tags.get(TAG_S3B_DEPARTURE))

    cursor.execute(INSERT_S3B, (
        str(order_id),
        int(bay_number) if bay_number is not None else None,
        arrival_time, start_filling_time,
        float(target_volume) if target_volume is not None else None,
        float(actual_volume) if actual_volume is not None else None,
        float(final_mass) if final_mass is not None else None,
        end_filling_time, departure_time,
    ))


def _insert_s4(cursor, tags: dict) -> None:
    order_id       = tags.get(TAG_ORDER_ID) or _next_order_id(cursor)
    gross_weight   = tags.get(TAG_S4_GROSS)
    net_weight     = tags.get(TAG_S4_NET)
    arrival_time   = _unix_sec_to_dt(tags.get(TAG_S4_ARRIVAL))
    departure_time = _unix_sec_to_dt(tags.get(TAG_S4_DEPARTURE))

    cursor.execute(INSERT_S4, (
        str(order_id),
        arrival_time,
        float(gross_weight) if gross_weight is not None else None,
        float(net_weight) if net_weight is not None else None,
        departure_time,
    ))


def _insert_exit(cursor, tags: dict) -> None:
    order_id       = tags.get(TAG_ORDER_ID) or _next_order_id(cursor)
    departure_time = _unix_sec_to_dt(tags.get(TAG_EXIT_DEPARTURE))
    cursor.execute(INSERT_EXIT, (str(order_id), departure_time))


STATION_INSERT_MAP = {
    "S2":   _insert_s2,
    "S3A":  _insert_s3a,
    "S3B":  _insert_s3b,
    "S4":   _insert_s4,
    "EXIT": _insert_exit,
}

# ---------------------------------------------------------------------------
# Azure Function trigger
# ---------------------------------------------------------------------------


@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="%IoTHubEventHubName%",
    connection="IoTHubEventHubConnectionString",
    cardinality="ONE",
    consumer_group="$Default",
)
def iot_hub_to_sql(event: func.EventHubEvent) -> None:
    logging.info(">>> iot_hub_to_sql triggered")

    body = event.get_body().decode("utf-8")
    logging.info("RAW BODY: %s", body)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        logging.error("JSON parse failed: %s", exc)
        return

    logging.info("Payload type: %s  length: %s", type(payload).__name__, len(payload) if isinstance(payload, list) else "n/a")

    try:
        logging.info("Connecting to SQL: server=%s database=%s user=%s", SQL_SERVER, SQL_DATABASE, SQL_USER)
        with get_connection() as conn:
            cursor = conn.cursor()
            for ddl in DDL_STATEMENTS:
                cursor.execute(ddl)

            if _is_plc_payload(payload):
                device_id = _get_connection_device_id(payload)
                logging.info("ConnectionDeviceId: '%s'", device_id)

                station = DEVICE_STATION_MAP.get(device_id)
                logging.info("Mapped station: '%s'", station)

                if station is None:
                    logging.warning("Unknown device id '%s' – not in DEVICE_STATION_MAP. Known keys: %s",
                                    device_id, list(DEVICE_STATION_MAP.keys()))
                    return

                tags = _extract_tags(payload)
                logging.info("Extracted tags: %s", tags)

                STATION_INSERT_MAP[station](cursor, tags)

            else:
                logging.info("Not a PLC payload – handling as test_device_telemetry")
                _handle_test_device(cursor, payload)

            conn.commit()
        logging.info("<<< Done: committed successfully")
    except pymssql.Error as exc:
        logging.error("SQL error (number=%s severity=%s): %s",
                      exc.args[0] if exc.args else "?",
                      exc.args[1] if len(exc.args) > 1 else "?", exc)
        raise
    except Exception as exc:
        logging.error("Unexpected error: %s", exc, exc_info=True)
        raise
