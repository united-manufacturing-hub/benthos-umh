#!/usr/bin/env python3
"""
Generate spec‑compliant Sparkplug‑B payloads and print Go constants.

Run:
    poetry run python gen_vectors.py   # or `python -m venv … && pip install …`

The script outputs ready‑to‑paste `const` assignments matching Go's
`base64.StdEncoding`.
"""
import base64
import sys
from datetime import datetime

from google.protobuf.timestamp_pb2 import Timestamp

# Eclipse‑Tahu proto
from sparkplug_b import Payload_pb2 as pb

# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def metric(name=None, alias=None, datatype=pb.Payload.Metric.DOUBLE, value=None):
    m = pb.Payload.Metric()
    if name is not None:
        m.name = name
    if alias is not None:
        m.alias = alias
    m.datatype = datatype
    # set value
    if datatype == pb.Payload.Metric.DOUBLE:
        m.double_value = float(value)
    elif datatype == pb.Payload.Metric.INT64:
        m.long_value = int(value)
    elif datatype == pb.Payload.Metric.BOOLEAN:
        m.boolean_value = bool(value)
    elif datatype == pb.Payload.Metric.STRING:
        m.string_value = str(value)
    else:
        raise ValueError("datatype not handled")
    return m


def payload(seq, *metrics):
    p = pb.Payload()
    ts = Timestamp()
    ts.GetCurrentTime()
    p.timestamp.CopyFrom(ts)
    p.seq = seq
    p.metrics.extend(metrics)
    return p


def b64(pl):
    return base64.b64encode(pl.SerializeToString()).decode("ascii")


def const(name, data):
    print(f'{name} = "{data}"')


# --------------------------------------------------------------------------- #
# NBIRTH / DBIRTH full (aliases 1‑3 plus NC/Rebirth)
# --------------------------------------------------------------------------- #
bdseq = metric("bdSeq", 0, pb.Payload.Metric.INT64, 1)
nc_rebirth = metric("Node Control/Rebirth", 0, pb.Payload.Metric.BOOLEAN, True)

m_temp = metric("Temperature", 1, pb.Payload.Metric.DOUBLE, 25.7)
m_press = metric("TisPressure", 2, pb.Payload.Metric.INT64, 42)
m_greet = metric("Greeting", 3, pb.Payload.Metric.STRING, "hi")

nbirth_full = payload(0, bdseq, nc_rebirth, m_temp, m_press, m_greet)
const("NBIRTH_full_v1", b64(nbirth_full))

# DBIRTH payload is identical
const("DBIRTH_full_v1", b64(nbirth_full))

# --------------------------------------------------------------------------- #
# Simple NDATA / DDATA (aliases 1 & 2 only)
# --------------------------------------------------------------------------- #
ndata = payload(1,  # seq 1
                metric(alias=1, datatype=pb.Payload.Metric.DOUBLE, value=25.7),
                metric(alias=2, datatype=pb.Payload.Metric.BOOLEAN, value=True))
const("NDATA_double_bool_v1", b64(ndata))
const("DDATA_double_bool_v1", b64(ndata))

# seq wrap‑around example (seq 255 → 0)
ndata_wrap = payload(0,  # seq 0
                     metric(alias=1, datatype=pb.Payload.Metric.DOUBLE, value=22.2),
                     metric(alias=2, datatype=pb.Payload.Metric.BOOLEAN, value=False))
const("NDATA_seq_wrap_v1", b64(ndata_wrap))

# --------------------------------------------------------------------------- #
# Minimal NDEATH (bdSeq only)
# --------------------------------------------------------------------------- #
ndeath = payload(0, bdseq)
const("NDEATH_min_v1", b64(ndeath))

# --------------------------------------------------------------------------- #
# NCMD / DCMD – Rebirth request
# --------------------------------------------------------------------------- #
ncmd = payload(0, metric("Node Control/Rebirth",
                         datatype=pb.Payload.Metric.BOOLEAN, value=True))
const("NCMD_rebirth_v1", b64(ncmd))
const("DCMD_rebirth_v1", b64(ncmd))

# --------------------------------------------------------------------------- #
print("\n// Generated at", datetime.utcnow().isoformat(), "Z", file=sys.stderr) 