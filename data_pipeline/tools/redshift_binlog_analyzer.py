# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import locale
import logging
import sys
from decimal import Decimal

import sqlparse
import staticconf
from terminaltables import AsciiTable
from yelp_redshift import client


class MissingSeconds(Exception):
    pass


def ensure_client_exists(func):
    def with_client_wrapper(self, *args, **kwargs):
        if self.client:
            return func(self, *args, **kwargs)

        with client.RedshiftClient() as self.client:
            return func(self, *args, **kwargs)
        self.client = None

    return with_client_wrapper


Table = AsciiTable


class RedshiftBinlogAnalyzer(object):
    table = "filtered_binlog_updates"
    unfiltered_table = "binlog_updates"

    events_per_second_table = "binlog_events_per_second"

    percentiles = ('0.99', '0.999', '0.9999', '0.99999')
    seconds = (2, 5, 10, 15, 20, 30, 60, 90, 120, 300)
    throughputs = range(2000, 6001, 250)

    def __init__(self):
        locale.setlocale(locale.LC_ALL, 'en_US')
        staticconf.YamlConfiguration('redshift_binlog_config.yaml', namespace='yelp_redshift')

        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
        self.client = None
        self.events_per_second_table_built = False

    @ensure_client_exists
    def analyze(self):
        self._check_and_rebuild_filtered_table()
        self._output_row_counts()
        self._validate_and_output_analysis_period()
        self.output_events_per_second_summary_stats()
        self.output_throughput_summary_table()
        self.output_events_per_second_window_stats()
        self.output_events_per_second_per_table()
        self.export_events_per_second()

    def _check_and_rebuild_filtered_table(self):
        if not self._is_filtered_table_up_to_date():
            self._rebuild_filtered_table()

    def _rebuild_filtered_table(self):
        logging.debug("Rebuilding filtered table, this will take a few minutes...")
        if self.client.table_exists(self.table):
            self.client.drop_table(self.table)
        self._execute("CREATE TABLE {table} (LIKE {unfiltered_table});")
        self._execute("""
            INSERT INTO {table}
                SELECT * FROM {unfiltered_table}
                    WHERE binlog_table NOT IN ('`yelp`.`cacheserv`') AND
                      binlog_table NOT LIKE '`yelp`.`__%_new`';
        """)

    def _output_row_counts(self):
        filtered_row_count = self._get_count(self.table)
        unfiltered_row_count = self._get_count(self.unfiltered_table)
        self._output_text(
            "Analyzing {filtered_rows} filtered row events from "
            "{unfiltered_rows} total events".format(
                filtered_rows=self._format_int(filtered_row_count),
                unfiltered_rows=self._format_int(unfiltered_row_count)
            )
        )

    def _validate_and_output_analysis_period(self):
        min_timestamp, max_timestamp, total_distinct_timestamps = self._execute(
            "SELECT MIN(timestamp), MAX(timestamp), COUNT(distinct(timestamp)) "
            "FROM {table}"
        ).fetchone()
        # min to max inclusive should match the number of timestamps we
        # have a count for
        if total_distinct_timestamps != (max_timestamp - min_timestamp + 1):
            raise MissingSeconds
        self._output_text(
            "Looking at {total_seconds} seconds from {start} to {end}".format(
                total_seconds=self._format_int(total_distinct_timestamps),
                start=min_timestamp,
                end=max_timestamp
            )
        )

    def _ensure_events_per_second_table_built(self):
        if not self.events_per_second_table_built:
            self._execute(
                """
                SELECT {timestamp_col} as event_timestamp,
                    COUNT(*) as count
                    INTO TEMP TABLE {events_per_second_table}
                    FROM {table}
                    GROUP BY event_timestamp
                    ORDER BY event_timestamp
                """,
                timestamp_col=self._format_column(self.table, 'timestamp'),
            )
            self.events_per_second_table_built = True

    def output_events_per_second_per_table(self):
        rows = self._execute(
            """
            SELECT {timestamp_col}, COUNT(*) as count, binlog_table FROM {table}
                GROUP BY {timestamp_col}, binlog_table
                HAVING COUNT(*) > 5000
                ORDER BY count DESC
                LIMIT 100
            """,
            timestamp_col=self._format_column(self.table, 'timestamp')
        ).fetchall()
        header = ['Timestamp', 'Events per Second', 'Table']
        rows = [
            [str(time), self._format_int(count), table]
            for time, count, table in rows
        ]
        self._output_text(
            Table(
                [header] + rows,
                "Tables with greatest events per second (EPS > 5,000)"
            )
        )

    @ensure_client_exists
    def export_events_per_second(self):
        self._ensure_events_per_second_table_built()
        with open('eps.csv', 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['event_timestamp', 'count'])
            result = self._execute(
                "SELECT event_timestamp, count FROM {events_per_second_table}"
            ).fetchall()
            for row in result:
                writer.writerow(list(row))

    @ensure_client_exists
    def output_events_per_second_summary_stats(self):
        self._ensure_events_per_second_table_built()
        self._output_summary_stats(
            'count',
            self.events_per_second_table,
            'Overall Events per Second'
        )

    @ensure_client_exists
    def output_throughput_summary_table(self):
        self._ensure_events_per_second_table_built()
        seconds_to_throughputs = {}
        for seconds in self.seconds:
            seconds_to_throughputs[seconds] = self._fetch_throughput_proportion(seconds)

        self._output_text(
            self._create_throughput_table(
                "Windows with EPS > Throughput",
                lambda seconds, throughput: self._format_percent(seconds_to_throughputs[seconds][throughput])
            )
        )

        def missed_window_frequency(seconds, throughput):
            proportion_missed = seconds_to_throughputs[seconds][throughput]
            num_missed_per_day = (86400.0 / seconds) * proportion_missed
            if proportion_missed == 0.0:
                return "Never"
            else:
                return "{missed}/day".format(missed=self._format_dec(num_missed_per_day))

        self._output_text(
            self._create_throughput_table(
                "Missed Window Frequency",
                missed_window_frequency
            )
        )

    def _create_throughput_table(self, title, format_func):
        header = ["Throughput"] + ["{sec} sec".format(sec=seconds) for seconds in self.seconds]

        table_data = [
            ["{throughput} EPS".format(throughput=self._format_int(throughput))] +
            [
                format_func(seconds, throughput)
                for seconds in self.seconds
            ]
            for throughput in self.throughputs
        ]
        return Table([header] + table_data, title)

    def _fetch_throughput_proportion(self, seconds):
        throughput_query = ', '.join(
            "count(CASE WHEN average_count > {throughput} THEN 1 END) as percent_above_{throughput}".format(
                throughput=throughput
            ) for throughput in self.throughputs
        )
        result = self._execute("""
            SELECT
                count(*) as total_count,
                {throughput_query}
            FROM ({avg_times_query})
        """.format(
            avg_times_query=self._average_times_query(seconds),
            throughput_query=throughput_query
        )).fetchone()
        result_dict = {}
        total_rows = float(result[0])
        for total_rows_above_throughput, throughput in zip(result[1:], self.throughputs):
            result_dict[throughput] = total_rows_above_throughput / total_rows
        return result_dict

    @ensure_client_exists
    def output_events_per_second_window_stats(self):
        self._ensure_events_per_second_table_built()
        for seconds in self.seconds:
            self._output_summary_stats(
                'average_count',
                self._average_times_query(seconds),
                "Average Events per {seconds} seconds".format(seconds=seconds)
            )

    def _output_summary_stats(self, column, select_from, title):
        average_eps, max_eps, sample_sd, pop_sd = self._fetch_summary_stats(
            column, select_from
        )

        percentile_values = self._fetch_percentiles(
            self.percentiles,
            column,
            select_from
        )

        header = ['Average EPS', 'Max EPS', 'Pop SD', '3-Sigma EPS', '6-Sigma EPS']
        header.extend([
            "{percentile}th %tile".format(
                percentile=(Decimal(percentile) * 100).normalize()
            )
            for percentile in self.percentiles
        ])

        data = [
            self._format_int(average_eps),
            self._format_int(max_eps),
            self._format_dec(pop_sd),
            self._format_dec(average_eps + (3 * pop_sd)),
            self._format_dec(average_eps + (6 * pop_sd)),
        ]
        data.extend([
            self._format_dec(percentile_values[percentile])
            for percentile in self.percentiles
        ])
        self._output_text(
            Table([header, data], title)
        )

    def _fetch_summary_stats(self, column, select_from):
        select_from = self._format_select_from(select_from)

        average_eps, max_eps, sample_sd, pop_sd = self._execute(
            """
            SELECT
                avg({column}),
                max({column}),
                CAST(stddev_samp({column}) as dec(14,2)),
                CAST(stddev_pop({column}) as dec(14,2))
            FROM {select_from}
            """,
            column=column,
            select_from=select_from
        ).fetchone()
        return average_eps, max_eps, sample_sd, pop_sd

    def _fetch_percentiles(self, percentiles, column, select_from):
        select_from = self._format_select_from(select_from)

        if not all(isinstance(percentile, str) for percentile in percentiles):
            raise TypeError("Percentiles should be strings because of floating point issues")

        percentile_query = ', '.join(
            "percentile_cont({percentile}) within GROUP (ORDER BY {column}) OVER ()".format(
                percentile=percentile,
                column=column
            ) for percentile in percentiles
        )
        result = self._execute(
            """
            SELECT
                {percentile_query}
            FROM {select_from}
            LIMIT 1
            """,
            percentile_query=percentile_query,
            select_from=select_from
        ).fetchone()
        return dict(zip(percentiles, result))

    def _average_times_query(self, seconds):
        return """
            SELECT * FROM (
                SELECT
                    AVG(count) OVER (ORDER BY event_timestamp ROWS {preceding_rows} PRECEDING) as average_count,
                    event_timestamp
                FROM {events_per_second_table}
            ) WHERE event_timestamp % {seconds} = 0
        """.format(
            seconds=seconds,
            events_per_second_table=self.events_per_second_table,
            preceding_rows=(seconds - 1)
        )

    def _format_select_from(self, select_from):
        if select_from.strip().lower().startswith('select '):
            select_from = "({select_statement})".format(
                select_statement=select_from
            )
        return select_from

    def _is_filtered_table_up_to_date(self):
        return (
            self.client.table_exists(self.table) and
            self._update_range(self.table) == self._update_range(self.unfiltered_table)
        )

    def _update_range(self, table):
        min_update_timestamp, max_update_timestamp = self._execute(
            "SELECT MIN(timestamp), MAX(timestamp) from {table};", table=table
        ).fetchone()
        return min_update_timestamp, max_update_timestamp

    def _get_count(self, table):
        (count,) = self._execute(
            "SELECT COUNT(*) FROM {table};", table=table
        ).fetchone()
        return count

    @ensure_client_exists
    def _execute(self, sql_str, **query_args):
        if 'table' not in query_args:
            query_args['table'] = self.table
        if 'unfiltered_table' not in query_args:
            query_args['unfiltered_table'] = self.unfiltered_table
        if 'events_per_second_table' not in query_args:
            query_args['events_per_second_table'] = self.events_per_second_table

        query = sql_str.format(**query_args)
        self._log_query(query)
        query = query.replace('%', '%%')
        return self.client.execute(query)

    def _log_query(self, query):
        logging.info(
            "Executing query:\n%s" %
            sqlparse.format(query, reindent=True, keyword_case='upper')
        )

    def _format_int(self, int_to_format):
        return locale.format("%d", int_to_format, grouping=True)

    def _format_dec(self, dec_to_format):
        return locale.format("%.2f", dec_to_format, grouping=True)

    def _format_percent(self, float_to_format):
        return locale.format("%.4f%%", float_to_format * 100.0, grouping=True)

    def _format_column(self, table, column):
        return '"{table}"."{column}"'.format(table=table, column=column)

    def _output_text(self, text):
        if isinstance(text, Table):
            print "\n{table}".format(table=text.table)
        else:
            print text

RedshiftBinlogAnalyzer().analyze()
