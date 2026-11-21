#!/usr/bin/env python3
"""
Shopping Sessions pipeline (DuckDB + scipy connected components).

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import glob
import json
import mmap
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import duckdb
import numpy as np
import pyarrow as pa
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

T0 = time.time()
DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

# Allow `pipeline.py DATA OUT` as used by run_fixtures.py.
if len(sys.argv) >= 3:
    DATA, OUT = sys.argv[-2], sys.argv[-1]


def log(msg):
    print(f"[{time.time() - T0:6.2f}s] {msg}", file=sys.stderr, flush=True)


con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.75))}GB'")
os.makedirs(SCRATCH, exist_ok=True)
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)


def f1_ms(s):
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


WS = f1_ms(manifest["window_start"])
WE = f1_ms(manifest["window_end"])
DAY = 86_400_000
WEEK = 7 * DAY
CWS = WS - WEEK  # click window start

# ---------------------------------------------------------------------------
# Raw line loading.
# DuckDB's CSV reader treats a lone '\r' as a line break, which the rules forbid.
# Files without '\r' (everything the generator writes) go through DuckDB's fast
# reader as single-column lines; files with '\r' are split on '\n' in Python.
# ---------------------------------------------------------------------------


def has_cr(path):
    with open(path, "rb") as fh:
        try:
            with mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ) as m:
                return m.find(b"\r") != -1
        except ValueError:  # empty file
            return False


def load_lines(name, sub):
    files = sorted(glob.glob(os.path.join(DATA, sub, "*.csv")))
    with ThreadPoolExecutor(max_workers=NCPU) as ex:
        flags = list(ex.map(has_cr, files))
    fast = [f for f, cr in zip(files, flags) if not cr]
    slow = [f for f, cr in zip(files, flags) if cr]
    parts = []
    if fast:
        flist = "[" + ",".join("'" + f.replace("'", "''") + "'" for f in fast) + "]"
        parts.append(
            f"SELECT line FROM read_csv({flist}, columns={{'line':'VARCHAR'}}, header=true, "
            f"delim=chr(1), quote='', escape='', auto_detect=false, strict_mode=false)"
        )
    if slow:
        lines = []
        for f in slow:
            with open(f, "rb") as fh:
                data = fh.read().decode("utf-8")
            rows = data.split("\n")
            if rows and rows[-1] == "":
                rows.pop()  # trailing newline ends the last line, not a new empty line
            lines.extend(rows[1:])  # drop the header line
        tbl = pa.table({"line": pa.array(lines, type=pa.string())})
        con.register(f"{name}_slow", tbl)
        parts.append(f"SELECT line FROM {name}_slow")
    if not parts:
        parts.append("SELECT NULL::VARCHAR AS line WHERE false")
    con.execute(f"CREATE OR REPLACE VIEW {name}_lines AS " + " UNION ALL ".join(parts))


# A field is either wholly quoted with no quote inside, or has no quote at all.
F = '("[^",]*"|[^",]*)'


def line_re(n):
    return "^" + ",".join([F] * n) + "$"


def fields(cols):
    """Struct-extract expression for a line with the given column names."""
    names = "[" + ",".join(f"'{c}'" for c in cols) + "]"
    return f"regexp_extract(line, '{line_re(len(cols))}', {names})"


# unquote + empty -> NULL
con.execute("""CREATE MACRO uq(s) AS nullif(replace(s, '"', ''), '')""")

# Timestamp parser: returns epoch ms (BIGINT) or NULL when the value is bad.
con.execute(r"""
CREATE MACRO ts_ms(s) AS (
  CASE
    WHEN s IS NULL THEN NULL
    WHEN regexp_full_match(s, '[0-9]+') THEN
      CASE WHEN length(ltrim(s, '0')) <= 15
           THEN CAST(CASE WHEN ltrim(s, '0') = '' THEN '0' ELSE ltrim(s, '0') END AS BIGINT)
           ELSE NULL END
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})') THEN
      CASE WHEN CAST(substr(s, 6, 2) AS INT) BETWEEN 1 AND 12
            AND CAST(substr(s, 9, 2) AS INT) BETWEEN 1 AND
                day(last_day(make_date(CAST(substr(s, 1, 4) AS INT),
                                       least(greatest(CAST(substr(s, 6, 2) AS INT), 1), 12), 1)))
            AND CAST(substr(s, 12, 2) AS INT) <= 23
            AND CAST(substr(s, 15, 2) AS INT) <= 59
            AND CAST(substr(s, 18, 2) AS INT) <= 59
            AND (length(s) = 20 OR (CAST(substr(s, 21, 2) AS INT) <= 23 AND CAST(substr(s, 24, 2) AS INT) <= 59))
      THEN epoch_ms(make_timestamp(CAST(substr(s, 1, 4) AS BIGINT), CAST(substr(s, 6, 2) AS BIGINT),
                                   CAST(substr(s, 9, 2) AS BIGINT), CAST(substr(s, 12, 2) AS BIGINT),
                                   CAST(substr(s, 15, 2) AS BIGINT), CAST(substr(s, 18, 2) AS DOUBLE)))
           - CASE WHEN length(s) = 20 THEN 0
                  ELSE (CASE WHEN substr(s, 20, 1) = '-' THEN -1 ELSE 1 END)
                       * (CAST(substr(s, 21, 2) AS BIGINT) * 60 + CAST(substr(s, 24, 2) AS BIGINT)) * 60000
             END
      ELSE NULL END
    ELSE NULL
  END)
""")

load_lines("ev", "events")
load_lines("ck", "clicks")
load_lines("lk", "links")
log("lines registered")

# ---------------------------------------------------------------------------
# Small dimension tables (clean).
# ---------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE products AS SELECT * FROM read_csv('{os.path.join(DATA, "products.csv")}',
  columns={{'product_id':'VARCHAR','category':'VARCHAR'}}, header=true, auto_detect=false)""")
con.execute(f"""
CREATE TABLE campaigns AS SELECT * FROM read_csv('{os.path.join(DATA, "campaigns.csv")}',
  columns={{'campaign_id':'VARCHAR','channel':'VARCHAR'}}, header=true, auto_detect=false)""")

# ---------------------------------------------------------------------------
# Linked IDs -> person name (smallest ID in its connected component).
# ---------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE links AS
SELECT uq(f.a) AS a, uq(f.b) AS b FROM (
  SELECT {fields(['a', 'b'])} AS f FROM lk_lines
  WHERE line IS NOT NULL AND regexp_full_match(line, '{line_re(2)}')
) WHERE uq(f.a) IS NOT NULL AND uq(f.b) IS NOT NULL
""")
con.execute("""
CREATE TABLE ids AS
SELECT id, (row_number() OVER (ORDER BY id) - 1)::BIGINT AS ix
FROM (SELECT a AS id FROM links UNION SELECT b FROM links)
""")
n_ids = con.execute("SELECT count(*) FROM ids").fetchone()[0]
if n_ids:
    edges = con.execute("""
      SELECT ia.ix AS x, ib.ix AS y FROM links
      JOIN ids ia ON ia.id = links.a JOIN ids ib ON ib.id = links.b
    """).fetchnumpy()
    x, y = edges["x"].astype(np.int64), edges["y"].astype(np.int64)
    g = coo_matrix((np.ones(len(x), dtype=np.int8), (x, y)), shape=(n_ids, n_ids))
    _, labels = connected_components(g, directed=True, connection="weak")
    minix = np.full(labels.max() + 1, n_ids, dtype=np.int64)
    np.minimum.at(minix, labels, np.arange(n_ids, dtype=np.int64))
    con.register("pmap_np", pa.table({"ix": np.arange(n_ids, dtype=np.int64), "pix": minix[labels]}))
    con.execute("""
      CREATE TABLE idmap AS
      SELECT i.id AS id, p.id AS person
      FROM ids i JOIN pmap_np m ON m.ix = i.ix JOIN ids p ON p.ix = m.pix
    """)
else:
    con.execute("CREATE TABLE idmap (id VARCHAR, person VARCHAR)")
log(f"links: {n_ids} ids")

# ---------------------------------------------------------------------------
# Events.
# ---------------------------------------------------------------------------

con.execute(f"""
CREATE TABLE ev_str AS
SELECT uq(x[1]) AS event_id, uq(x[2]) AS user_id, uq(x[3]) AS ts, uq(x[4]) AS ingest_ts,
       uq(x[5]) AS event_type, uq(x[6]) AS product_id, uq(x[7]) AS price_cents
FROM (SELECT string_split(line, ',') AS x FROM ev_lines
      WHERE line IS NOT NULL AND regexp_full_match(line, '{line_re(7)}'))
WHERE uq(x[1]) IS NOT NULL
""")
log("events split")
# One row per event_id: the copy with the smallest ingest_ts (copies with equal ingest_ts are identical).
con.execute("""
CREATE TABLE ev_dedup AS
SELECT unnest(r) FROM (
  SELECT arg_min({'user_id': user_id, 'ts': ts, 'it': epoch_ms(strptime(ingest_ts, '%Y-%m-%dT%H:%M:%SZ')),
                  'event_type': event_type, 'product_id': product_id, 'price_cents': price_cents},
                 ingest_ts) AS r
  FROM ev_str GROUP BY event_id)
""")
con.execute("DROP TABLE ev_str")
log("events deduped")
con.execute(f"""
CREATE TABLE ev_valid AS
SELECT user_id, ts_ms(ts) AS t, it, event_type = 'purchase' AS is_purchase,
       CASE WHEN event_type = 'purchase' THEN price_cents END AS price_s, product_id
FROM ev_dedup
WHERE user_id IS NOT NULL AND product_id IS NOT NULL AND event_type IN ('view', 'cart', 'purchase')
""")
con.execute(f"""
CREATE TABLE ev_clean AS
SELECT coalesce(m.person, v.user_id) AS person, v.t AS ts, v.is_purchase,
       CASE WHEN v.is_purchase THEN TRY_CAST(ltrim(v.price_s, '0') AS BIGINT) ELSE 0 END AS price,
       coalesce(p.category, 'unknown') AS category
FROM ev_valid v
LEFT JOIN idmap m ON m.id = v.user_id
LEFT JOIN products p ON p.product_id = v.product_id
WHERE v.t >= {WS} AND v.t < {WE}
  AND v.it - v.t <= {DAY}
  AND (NOT v.is_purchase OR (
        v.price_s IS NOT NULL AND regexp_full_match(v.price_s, '[0-9]+')
        AND length(ltrim(v.price_s, '0')) BETWEEN 1 AND 8
        AND TRY_CAST(ltrim(v.price_s, '0') AS BIGINT) BETWEEN 1 AND 10000000))
""")
log("events cleaned")

# Sessions.
con.execute(f"""
CREATE TABLE ev_s AS
WITH g AS (
  SELECT *, CASE WHEN ts - lag(ts) OVER w > 1800000 OR lag(ts) OVER w IS NULL THEN 1 ELSE 0 END AS new_s
  FROM ev_clean WINDOW w AS (PARTITION BY person ORDER BY ts)
)
SELECT *, sum(new_s) OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sid FROM g
""")
con.execute("""
CREATE TABLE sessions AS
SELECT person, sid, min(ts) AS start, CAST(epoch_ms(min(ts)) AS DATE) AS day,
       count(*) AS n_events, sum(price) AS revenue, bool_or(is_purchase) AS has_purchase
FROM ev_s GROUP BY person, sid
""")
log("sessions built")

# ---------------------------------------------------------------------------
# Clicks.
# ---------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE ck_str AS
SELECT uq(x[1]) AS click_id, uq(x[2]) AS user_id, uq(x[3]) AS campaign_id, uq(x[4]) AS ts
FROM (SELECT string_split(line, ',') AS x FROM ck_lines
      WHERE line IS NOT NULL AND regexp_full_match(line, '{line_re(4)}'))
WHERE uq(x[1]) IS NOT NULL
""")
con.execute(f"""
CREATE TABLE clicks AS
WITH d AS (
  SELECT click_id, any_value(user_id) AS user_id, any_value(campaign_id) AS campaign_id, any_value(ts) AS ts
  FROM ck_str GROUP BY click_id
), v AS (
  SELECT click_id, user_id, campaign_id, ts_ms(ts) AS t FROM d
  WHERE user_id IS NOT NULL AND campaign_id IN (SELECT campaign_id FROM campaigns)
)
SELECT coalesce(m.person, v.user_id) AS person, v.click_id, v.campaign_id, v.t AS ts
FROM v LEFT JOIN idmap m ON m.id = v.user_id
WHERE v.t >= {CWS} AND v.t < {WE}
""")
log("clicks cleaned")

# Credit: latest click at or before the session start (ties: smallest click_id), within 7 days.
con.execute(f"""
CREATE TABLE credited AS
WITH cb AS (
  SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clicks GROUP BY person, ts
)
SELECT s.person, s.sid, s.day, s.has_purchase, s.revenue,
       CASE WHEN cb.ts IS NOT NULL AND s.start - cb.ts <= {WEEK} THEN cb.campaign_id ELSE 'organic' END AS campaign_id
FROM sessions s ASOF LEFT JOIN cb ON s.person = cb.person AND s.start >= cb.ts
""")

# Assists: merge each (person, campaign)'s click intervals [ts, ts+7d] into disjoint
# ranges, then count sessions whose start falls in a range.
con.execute(f"""
CREATE TABLE assist_iv AS
WITH c AS (
  SELECT person, campaign_id, ts,
         CASE WHEN ts - lag(ts) OVER w > {WEEK} OR lag(ts) OVER w IS NULL THEN 1 ELSE 0 END AS new_g
  FROM clicks WINDOW w AS (PARTITION BY person, campaign_id ORDER BY ts)
), g AS (
  SELECT *, sum(new_g) OVER (PARTITION BY person, campaign_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS gid FROM c
)
SELECT person, campaign_id, min(ts) AS lo, max(ts) + {WEEK} AS hi FROM g GROUP BY person, campaign_id, gid
""")
con.execute("""
CREATE TABLE assists AS
SELECT iv.campaign_id, s.day, count(*) AS assisted_sessions
FROM sessions s JOIN assist_iv iv ON s.person = iv.person AND s.start >= iv.lo AND s.start <= iv.hi
GROUP BY ALL
""")
log("attribution done")

# ---------------------------------------------------------------------------
# Outputs.
# ---------------------------------------------------------------------------
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

con.execute(f"""
COPY (
  WITH ud AS (
    SELECT person, day, count(*) AS sessions, sum(n_events) AS events, sum(revenue) AS revenue_cents
    FROM sessions GROUP BY person, day
  ), cat AS (
    SELECT s.person, s.day, e.category, sum(e.price) AS rev
    FROM ev_s e JOIN sessions s ON s.person = e.person AND s.sid = e.sid
    WHERE e.is_purchase GROUP BY ALL
  ), top AS (
    SELECT person, day, category FROM cat
    QUALIFY row_number() OVER (PARTITION BY person, day ORDER BY rev DESC, category ASC) = 1
  )
  SELECT ud.person AS user_id, ud.day, ud.sessions::BIGINT AS sessions, ud.events::BIGINT AS events,
         ud.revenue_cents::BIGINT AS revenue_cents, top.category AS top_category
  FROM ud LEFT JOIN top ON top.person = ud.person AND top.day = ud.day
) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")

con.execute(f"""
COPY (
  WITH cr AS (
    SELECT campaign_id, day, count(*) AS sessions,
           count(*) FILTER (WHERE has_purchase) AS purchasing_sessions, sum(revenue) AS revenue_cents
    FROM credited GROUP BY ALL
  ), k AS (
    SELECT campaign_id, day FROM cr UNION SELECT campaign_id, day FROM assists
  )
  SELECT k.campaign_id,
         CASE WHEN k.campaign_id = 'organic' THEN 'organic' ELSE c.channel END AS channel,
         k.day,
         coalesce(cr.sessions, 0)::BIGINT AS sessions,
         coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
         coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
         coalesce(a.assisted_sessions, 0)::BIGINT AS assisted_sessions
  FROM k
  LEFT JOIN cr ON cr.campaign_id = k.campaign_id AND cr.day = k.day
  LEFT JOIN assists a ON a.campaign_id = k.campaign_id AND a.day = k.day
  LEFT JOIN campaigns c ON c.campaign_id = k.campaign_id
) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
log("done")
