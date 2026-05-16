import json
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import List

import azure.functions as func
import pymssql
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

_TZ7 = timezone(timedelta(hours=7))

# Sensor tag substrings to capture for telemetry blob output
TELEMETRY_PATTERNS = [
    'FT_100_1', 'FT_100_2', 'FT_100_3', 'FT_100_4',
    'FT_200_1', 'FT_200_2', 'TT_100', 'TT_200', 'VT_100', 'VT_200',
]

# (bay_number, po_tag, entry_tag, fill_start_tag, fill_end_tag, exit_tag)
_S3A_BAYS = [
    (1, 's3a1po', 'EntryTime_S3a1', 'FillStartTime_S3a1', 'FillEndTime_S3a1', 'ExitTime_S3a1'),
    (2, 's3a2po', 'EntryTime_S3a2', 'FillStartTime_S3a2', 'FillEndTime_S3a2', 'ExitTime_S3a2'),
    (3, 's3a3po', 'EntryTime_S3a3', 'FillStartTime_S3a3', 'FillEndTime_S3a3', 'ExitTime_S3a3'),
    (4, 's3a4po', 'EntryTime_S3a4', 'FillStartTime_S3a4', 'FillEndTime_S3a4', 'ExitTime_S3a4'),
]

_S3B_BAYS = [
    (5, 's3b1po', 'EntryTime_S3b1', 'FillStartTime_S3b1', 'FillEndTime_S3b1', 'ExitTime_S3b1'),
    (6, 's3b2po', 'EntryTime_S3b2', 'FillStartTime_S3b2', 'FillEndTime_S3b2', 'ExitTime_S3b2'),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _epoch_ms(ms):
    """Millisecond Unix timestamp → UTC+7 datetime (used for telemetry .t field)."""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone(_TZ7)


def _epoch_s(s):
    """Second Unix timestamp → UTC+7 datetime (used for transaction .v fields)."""
    return datetime.fromtimestamp(int(s), tz=timezone.utc).astimezone(_TZ7)


def _fmt(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def _find(values, pattern):
    """Return first item whose id contains pattern, or None."""
    for item in values:
        if pattern in item.get('id', ''):
            return item
    return None


def _get_device_id(event, body):
    meta = event.metadata or {}
    return (
        meta.get('iothub-connection-device-id')
        or body.get('IoTHub', {}).get('ConnectionDeviceId', '')
    )


def _sql_conn():
    return pymssql.connect(
        server=os.environ['SqlServer'],
        user=os.environ['SqlUser'],
        password=os.environ['SqlPassword'],
        database=os.environ['SqlDatabase'],
    )


# ---------------------------------------------------------------------------
# Azure Function — batch Event Hub trigger
# ---------------------------------------------------------------------------

@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name="%IoTHubEventHubName%",
    connection="IoTHubEventHubConnectionString",
    cardinality="many",
    consumer_group="$Default",
)
def iot_hub_processor(events: List[func.EventHubEvent]) -> None:
    telemetry_rows = []
    sql_inserts = []        # list of (table_name, params_tuple)
    test_device_rows = []   # list of (temperature, humidity, received_at)

    for event in events:
        try:
            body = json.loads(event.get_body().decode())
        except Exception as exc:
            logging.warning("Skipping unparseable event: %s", exc)
            continue

        device_id = _get_device_id(event, body)
        values = body.get('values', [])

        if device_id == 'test-device':
            _extract_test_device(values, test_device_rows)
            continue

        if device_id != 'S2_plc':
            continue

        # --- Telemetry → Blob ---
        for item in values:
            tag_id = item.get('id', '')
            for pat in TELEMETRY_PATTERNS:
                if pat in tag_id:
                    try:
                        telemetry_rows.append({
                            'tag_name':   tag_id,
                            'tag_value':  float(item['v']),
                            'received_at': _fmt(_epoch_ms(int(item['t']))),
                        })
                    except (KeyError, TypeError, ValueError) as exc:
                        logging.warning("Telemetry parse error %s: %s", tag_id, exc)
                    break

        # --- S2 Inbound Weigh Bridge → SQL ---
        _extract_weigh(values, sql_inserts,
                       label='S2',
                       table=os.environ.get('SqlTableS2', 'S2_LOGS'),
                       po_pat='S2po',         wt_pat='WT_001',
                       entry_pat='EntryTime_S2',
                       wstart_pat='WeighStartTime_S2',
                       wend_pat='WeighEndTime_S2',
                       exit_pat='ExitTime_S2',
                       weight_col='tare_weight_kg')

        # --- S3A Diesel Bays 1-4 → SQL ---
        for bay, po, entry, fstart, fend, exit_ in _S3A_BAYS:
            _extract_bay(values, sql_inserts,
                         table=os.environ.get('SqlTableS3a', 'S3A_STATIC_LOGS'),
                         bay_number=bay,
                         po_pat=po, entry_pat=entry,
                         fstart_pat=fstart, fend_pat=fend, exit_pat=exit_)

        # --- S3B Gasohol95 Bays 5-6 → SQL ---
        for bay, po, entry, fstart, fend, exit_ in _S3B_BAYS:
            _extract_bay(values, sql_inserts,
                         table=os.environ.get('SqlTableS3b', 'S3B_STATIC_LOGS'),
                         bay_number=bay,
                         po_pat=po, entry_pat=entry,
                         fstart_pat=fstart, fend_pat=fend, exit_pat=exit_)

        # --- S4 Outbound Weigh Bridge → SQL ---
        _extract_weigh(values, sql_inserts,
                       label='S4',
                       table=os.environ.get('SqlTableS4', 'S4_LOGS'),
                       po_pat='s4po',         wt_pat='WT_002',
                       entry_pat='EntryTime_S4',
                       wstart_pat='WeighStartTime_S4',
                       wend_pat='WeighEndTime_S4',
                       exit_pat='ExitTime_S4',
                       weight_col='gross_weight_kg')

    if telemetry_rows:
        _write_blob(telemetry_rows)

    if sql_inserts:
        _write_sql(sql_inserts)

    if test_device_rows:
        _write_test_device_sql(test_device_rows)


# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------

def _extract_weigh(values, out, label, table,
                   po_pat, wt_pat, entry_pat, wstart_pat, wend_pat, exit_pat, weight_col):
    po     = _find(values, po_pat)
    entry  = _find(values, entry_pat)
    wt     = _find(values, wt_pat)
    wstart = _find(values, wstart_pat)
    wend   = _find(values, wend_pat)
    exit_  = _find(values, exit_pat)
    if not all([po, entry, wt, wstart, wend, exit_]):
        return
    try:
        out.append((table, (
            str(po['v']),
            _fmt(_epoch_s(entry['v'])),
            float(wt['v']),
            _fmt(_epoch_s(wstart['v'])),
            _fmt(_epoch_s(wend['v'])),
            _fmt(_epoch_s(exit_['v'])),
        )))
        logging.info("%s row queued: order_id=%s", label, po['v'])
    except (KeyError, TypeError, ValueError) as exc:
        logging.warning("%s extract error: %s", label, exc)


def _extract_bay(values, out, table, bay_number,
                 po_pat, entry_pat, fstart_pat, fend_pat, exit_pat):
    po     = _find(values, po_pat)
    entry  = _find(values, entry_pat)
    fstart = _find(values, fstart_pat)
    fend   = _find(values, fend_pat)
    exit_  = _find(values, exit_pat)
    if not all([po, entry, fstart, fend, exit_]):
        return
    try:
        out.append((table, (
            str(po['v']),
            bay_number,
            _fmt(_epoch_s(entry['v'])),
            _fmt(_epoch_s(fstart['v'])),
            _fmt(_epoch_s(fend['v'])),
            _fmt(_epoch_s(exit_['v'])),
        )))
        logging.info("Bay %d row queued: order_id=%s", bay_number, po['v'])
    except (KeyError, TypeError, ValueError) as exc:
        logging.warning("Bay %d extract error: %s", bay_number, exc)


# ---------------------------------------------------------------------------
# Test-device extractor & writer
# ---------------------------------------------------------------------------

def _extract_test_device(values, out):
    tt100 = _find(values, 'TT_100')
    tt200 = _find(values, 'TT_200')
    if not (tt100 and tt200):
        logging.warning("test-device: missing TT_100 or TT_200 — skipping")
        return
    try:
        out.append((
            float(tt100['v']),
            float(tt200['v']),
            _fmt(_epoch_ms(int(tt100['t']))),
        ))
    except (KeyError, TypeError, ValueError) as exc:
        logging.warning("test-device extract error: %s", exc)


def _write_test_device_sql(rows):
    table = os.environ.get('SqlTableTestDevice', 'test_device_telemetry')
    sql = (
        f"INSERT INTO {table} (device_id, temperature, humidity, received_at) "
        "VALUES (%s, %s, %s, %s)"
    )
    try:
        conn = _sql_conn()
        cursor = conn.cursor()
        for temp, humid, received_at in rows:
            cursor.execute(sql, ('test-device', temp, humid, received_at))
        conn.commit()
        logging.info("test-device SQL committed: %d rows", len(rows))
    except Exception as exc:
        logging.error("test-device SQL write failed: %s", exc)
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def _write_blob(rows):
    conn_str  = os.environ['BlobConnectionString']
    container = os.environ['BlobContainerName']
    now = datetime.now(tz=_TZ7)
    blob_name = f"{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/{uuid.uuid4()}.jsonl"
    content = '\n'.join(json.dumps(r) for r in rows)
    try:
        BlobServiceClient.from_connection_string(conn_str) \
            .get_blob_client(container=container, blob=blob_name) \
            .upload_blob(content.encode(), overwrite=True)
        logging.info("Wrote %d telemetry rows → %s/%s", len(rows), container, blob_name)
    except Exception as exc:
        logging.error("Blob write failed: %s", exc)
        raise


def _write_sql(inserts):
    weigh_sql = (
        "INSERT INTO {table} "
        "(order_id, arrival_time, {weight_col}, start_weighting_time, end_weighting_time, departure_time) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )
    bay_sql = (
        "INSERT INTO {table} "
        "(order_id, bay_number, arrival_time, start_filling_time, end_filling_time, departure_time) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )

    s2_table  = os.environ.get('SqlTableS2',  'S2_LOGS')
    s4_table  = os.environ.get('SqlTableS4',  'S4_LOGS')
    s3a_table = os.environ.get('SqlTableS3a', 'S3A_STATIC_LOGS')
    s3b_table = os.environ.get('SqlTableS3b', 'S3B_STATIC_LOGS')

    try:
        conn = _sql_conn()
        cursor = conn.cursor()
        for table, params in inserts:
            if table == s2_table:
                sql = weigh_sql.format(table=table, weight_col='tare_weight_kg')
            elif table == s4_table:
                sql = weigh_sql.format(table=table, weight_col='gross_weight_kg')
            elif table in (s3a_table, s3b_table):
                sql = bay_sql.format(table=table)
            else:
                logging.warning("Unknown table %s — skipping", table)
                continue
            try:
                cursor.execute(sql, params)
            except pymssql.IntegrityError:
                logging.warning("Duplicate row skipped in %s: order_id=%s", table, params[0])
        conn.commit()
        logging.info("SQL committed: %d rows", len(inserts))
    except Exception as exc:
        logging.error("SQL write failed: %s", exc)
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
