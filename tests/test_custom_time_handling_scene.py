from __future__ import annotations

import unittest

import ibis
from ibis import _, table

from ibis_dsql import to_sql

TableInt64 = table(
    schema=dict(
        name="string",
        ts="int64",
    ),
    name="TableInt64"
)
TableInt64 = TableInt64.mutate(ts=TableInt64.ts.cast("timestamp"))

TableTimestamp = table(
    schema=dict(
        name="string",
        ts="timestamp",
    ),
    name="TableTimestamp"
)

class TimeSqlTest(unittest.TestCase):
    def test_date_from_parts_of_mutate_timestamp_column(self):
        start_time = ibis.date(2026, 1, 3)
        filtered = TableInt64.filter(TableInt64["ts"] >= start_time)
        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts AS ts FROM TableInt64 AS t0 WHERE t0.ts >= (UNIX_TIMESTAMP(CAST(CONCAT(CONCAT(CONCAT(CONCAT(CAST(2026 AS STRING), '-'), LPAD(CAST(1 AS STRING), 2, '0')), '-'), LPAD(CAST(3 AS STRING), 2, '0')) AS DATE)) * 1000)",
        )

    def test_date_from_parts_of_native_timestamp_column(self):
        start_time = ibis.date(2026, 1, 3)
        filtered = TableTimestamp.filter(TableTimestamp["ts"] >= start_time)
        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts >= CAST(CONCAT(CONCAT(CONCAT(CONCAT(CAST(2026 AS STRING), '-'), LPAD(CAST(1 AS STRING), 2, '0')), '-'), LPAD(CAST(3 AS STRING), 2, '0')) AS DATE)",
        )

    def test_timestamp_from_parts_of_mutate_timestamp_column(self):
        start_time = ibis.timestamp(2026, 1, 3, 10, 30, 0)
        filtered = TableInt64.filter(TableInt64["ts"] >= start_time)
        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts AS ts FROM TableInt64 AS t0 WHERE t0.ts >= (UNIX_TIMESTAMP(CAST(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CAST(2026 AS STRING), '-'), LPAD(CAST(1 AS STRING), 2, '0')), '-'), LPAD(CAST(3 AS STRING), 2, '0')), ' '), LPAD(CAST(10 AS STRING), 2, '0')), ':'), LPAD(CAST(30 AS STRING), 2, '0')), ':'), LPAD(CAST(0 AS STRING), 2, '0')) AS TIMESTAMP)) * 1000)",
        )

    def test_timestamp_from_parts_of_native_timestamp_column(self):
        start_time = ibis.timestamp(2026, 1, 3, 10, 30, 0)
        filtered = TableTimestamp.filter(TableTimestamp["ts"] >= start_time)
        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts >= CAST(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CAST(2026 AS STRING), '-'), LPAD(CAST(1 AS STRING), 2, '0')), '-'), LPAD(CAST(3 AS STRING), 2, '0')), ' '), LPAD(CAST(10 AS STRING), 2, '0')), ':'), LPAD(CAST(30 AS STRING), 2, '0')), ':'), LPAD(CAST(0 AS STRING), 2, '0')) AS TIMESTAMP)",
        )

    def test_time_to_string_of_mutate_timestamp_column(self):
        yesterday = ibis.now() - ibis.interval(days=1)
        start_time = ibis.timestamp(yesterday.strftime("%Y-%m-%d"))
        filtered = TableInt64.filter(TableInt64["ts"] >= start_time)

        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t2.name, t2.ts FROM (SELECT t1.name, t1.ts FROM (SELECT t0.name, t0.ts AS ts FROM TableInt64 AS t0) AS t1 WHERE t1.ts >= (UNIX_TIMESTAMP(CAST(TO_CHAR(CURRENT_TIMESTAMP - INTERVAL '1' DAY, 'YYYY-MM-DD') AS TIMESTAMP)) * 1000)) AS t2",
        )

    def test_time_to_string_of_native_timestamp_column(self):
        yesterday = ibis.now() - ibis.interval(days=1)
        start_time = ibis.timestamp(yesterday.strftime("%Y-%m-%d"))
        filtered = TableTimestamp.filter(TableTimestamp["ts"] >= start_time)

        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t1.name, t1.ts FROM (SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts >= CAST(TO_CHAR(CURRENT_TIMESTAMP - INTERVAL '1' DAY, 'YYYY-MM-DD') AS TIMESTAMP)) AS t1",
        )

    def test_extract_hour_of_mutate_timestamp_column(self):
        start_time = ibis.timestamp("2026-01-28 01:00:00")
        filtered = TableInt64.filter(TableInt64["ts"] >= start_time)

        expr = (filtered.mutate(hour=filtered["ts"].hour())
                .group_by("hour")
                .aggregate(cnt=_.count())
                .order_by("hour"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t2.hour, t2.cnt FROM (SELECT t1.hour, COUNT(*) AS cnt FROM (SELECT t0.name, t0.ts AS ts, EXTRACT(hour FROM CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS hour FROM TableInt64 AS t0 WHERE t0.ts >= (UNIX_TIMESTAMP(CAST('2026-01-28 01:00:00' AS TIMESTAMP)) * 1000)) AS t1 GROUP BY 1) AS t2 ORDER BY t2.hour ASC",
        )

    def test_extract_hour_of_native_timestamp_column(self):
        start_time = ibis.timestamp("2026-01-28 01:00:00")
        filtered = TableTimestamp.filter(TableTimestamp["ts"] >= start_time)

        expr = (filtered.mutate(hour=filtered["ts"].hour())
                .group_by("hour")
                .aggregate(cnt=_.count())
                .order_by("hour"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t2.hour, t2.cnt FROM (SELECT t1.hour, COUNT(*) AS cnt FROM (SELECT t0.name, t0.ts, EXTRACT(hour FROM t0.ts) AS hour FROM TableTimestamp AS t0 WHERE t0.ts >= CAST('2026-01-28 01:00:00' AS TIMESTAMP)) AS t1 GROUP BY 1) AS t2 ORDER BY t2.hour ASC",
        )

    def test_time_between_of_mutate_timestamp_column(self):
        start_time = ibis.timestamp("2026-01-28 01:00:00")
        end_time = ibis.timestamp("2026-01-28 09:00:00")
        filtered = TableInt64.filter(TableInt64.ts.between(start_time, end_time))

        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts AS ts FROM TableInt64 AS t0 WHERE t0.ts BETWEEN UNIX_TIMESTAMP(CAST('2026-01-28 01:00:00' AS TIMESTAMP)) * 1000 AND UNIX_TIMESTAMP(CAST('2026-01-28 09:00:00' AS TIMESTAMP)) * 1000",
        )

    def test_time_between_of_native_timestamp_column(self):
        start_time = ibis.timestamp("2026-01-28 01:00:00")
        end_time = ibis.timestamp("2026-01-28 09:00:00")
        filtered = TableTimestamp.filter(TableTimestamp.ts.between(start_time, end_time))

        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts BETWEEN CAST('2026-01-28 01:00:00' AS TIMESTAMP) AND CAST('2026-01-28 09:00:00' AS TIMESTAMP)",
        )

    def test_truncate_date_of_mutate_timestamp_column(self):
        start_time = ibis.now().date() - ibis.interval(days=5)
        end_time = ibis.now()
        expr = (TableInt64.filter((_.ts >= start_time) & (_.ts < end_time))
                    .group_by(day=_.ts.date())
                    .aggregate(cnt=_.count())
                    .order_by("day"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t3.day, t3.cnt FROM (SELECT DATE_TRUNC('DAY', CAST(FROM_UNIXTIME(CAST(t2.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS day, COUNT(*) AS cnt FROM (SELECT t1.name, t1.ts FROM (SELECT t0.name, t0.ts AS ts FROM TableInt64 AS t0) AS t1 WHERE t1.ts >= (UNIX_TIMESTAMP(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) - INTERVAL '5' DAY) * 1000) AND t1.ts < (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) * 1000)) AS t2 GROUP BY 1) AS t3 ORDER BY t3.day ASC",
        )

    def test_truncate_date_of_native_timestamp_column(self):
        start_time = ibis.now().date() - ibis.interval(days=5)
        end_time = ibis.now()
        expr = (TableTimestamp.filter((_.ts >= start_time) & (_.ts < end_time))
                    .group_by(day=_.ts.date())
                    .aggregate(cnt=_.count())
                    .order_by("day"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t2.day, t2.cnt FROM (SELECT DATE_TRUNC('DAY', t1.ts) AS day, COUNT(*) AS cnt FROM (SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts >= (DATE_TRUNC('DAY', CURRENT_TIMESTAMP) - INTERVAL '5' DAY) AND t0.ts < CURRENT_TIMESTAMP) AS t1 GROUP BY 1) AS t2 ORDER BY t2.day ASC",
        )

    def test_truncate_week_of_mutate_timestamp_column(self):
        start_time = ibis.now().truncate("week")
        end_time = ibis.now()
        expr = TableInt64.filter((_.ts >= start_time) & (_.ts < end_time)).select(_.name)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t2.name FROM (SELECT t1.name, t1.ts FROM (SELECT t0.name, t0.ts AS ts FROM TableInt64 AS t0) AS t1 WHERE t1.ts >= (UNIX_TIMESTAMP(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP)) * 1000) AND t1.ts < (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) * 1000)) AS t2",
        )

    def test_truncate_week_of_native_timestamp_column(self):
        start_time = ibis.now().truncate("week")
        end_time = ibis.now()
        expr = TableTimestamp.filter((_.ts >= start_time) & (_.ts < end_time)).select(_.name)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t1.name FROM (SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts >= DATE_TRUNC('WEEK', CURRENT_TIMESTAMP) AND t0.ts < CURRENT_TIMESTAMP) AS t1",
        )
