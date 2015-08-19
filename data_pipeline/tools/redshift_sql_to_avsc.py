# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import optparse
import re

import simplejson as json
import yelp_batch
from cached_property import cached_property
from yelp_batch.batch import batch_command_line_options
from yelp_batch.batch import os

from data_pipeline.tools._glob_util import get_file_paths_from_glob_patterns
# See https://regex101.com/r/kC0kZ1

CREATE_TABLE_REGEX = re.compile('^create(\s*table)?\s*((.+)\.)?(\w+)\s*\(?')

# See https://regex101.com/r/zG9kV1
PRIMARY_KEY_REGEX = re.compile('^primary\s*key\s*\((.+)?\)')

# See https://regex101.com/r/kD8iN5
FIELD_LINE_REGEX = re.compile(
    '^(\w+)\s*(\w+)\s*(\(\s*(\d+|\d+\s*\,\s*\d+)\s*\))?\s*(not\s+null|null)?.*\,'  # noqa
)

# See https://regex101.com/r/bN3xL0
START_FIELDS_REGEX = re.compile('^.*\(')

# See https://regex101.com/r/bR7bH2
STOP_FIELDS_REGEX = re.compile('^\)')


def _sanitize_line(line):
    return line.strip().lower()


class RedshiftFieldLineToAvroFieldConverter(object):

    def __init__(self, field_line, pkeys):
        """
        Args:
            field_line(string): Content of a column definition line from a
                redshift *.sql file
            pkeys([string]): A list of the primary keys, used for determining
                the meta attribute of "pkey"
        """
        self.field_line = _sanitize_line(field_line)
        self.pkeys = pkeys

    @cached_property
    def avro_field(self):
        field = {
            "name": self.name,
            "type": self.avro_type,
            "doc": ""
        }
        field.update(self.avro_meta_attributes)
        return field

    @cached_property
    def name(self):
        return self._regex_matcher.group(1)

    @cached_property
    def avro_type(self):
        sql_type = self.sql_type
        mapping = {
            'char': 'string',
            'varchar': 'string',
            'integer': 'int',
            'int': 'int',
            'int4': 'int',
            'smallint': 'int',
            'int2': 'int',
            'bigint': 'long',
            'int8': 'long',
            'real': 'float',
            'float4': 'float',
            'double': 'double',
            'float8': 'double',
            'float': 'double',
            'text': 'string',
            'date': 'string',
            'timestamp': 'long',
            'decimal': 'double',
            'boolean': 'boolean'
        }
        avro_type = mapping[sql_type]
        if self.nullable:
            return [avro_type, 'null']
        else:
            return avro_type

    @cached_property
    def sql_type(self):
        return self._regex_matcher.group(2)

    @cached_property
    def nullable(self):
        nullable_str = self._regex_matcher.group(5)
        return not(nullable_str and re.search('^(not\s+null)', nullable_str))

    @cached_property
    def avro_meta_attributes(self):
        meta = {}
        field_name = self.name
        for index, pkey_name in enumerate(self.pkeys):
            if pkey_name == field_name:
                meta['pkey'] = index + 1
        sql_type = self.sql_type
        sql_width = self.sql_type_width
        if sql_type == 'varchar':
            meta['maxlen'] = sql_width
        if sql_type == 'char':
            meta['fixlen'] = sql_width
        if sql_type == 'date' or sql_type == 'timestamp':
            meta[sql_type] = True
        if sql_type == 'decimal':
            meta['fixed_pt'] = True
            meta['precision'] = sql_width[0]
            meta['scale'] = sql_width[1]
        return meta

    @cached_property
    def sql_type_width(self):
        width = self._regex_matcher.group(4)
        if width:
            if ',' in width:
                return [
                    int(part.strip())
                    for part in width.split(',')
                ]
            else:
                return int(width)
        else:
            return None

    @cached_property
    def _regex_matcher(self):
        return FIELD_LINE_REGEX.search(self.field_line)


class RedshiftSQLToAVSCConverter(object):
    """ Simple converter from redshift *.sql CREATE TABLE definitions (such
    as those in yelp-main/schema/yelp_dw_redshift/tables) to data pipeline
    format Avro *.avsc schemas.

    Notes:
        This makes a number of assumptions about the input content, namely
        that there is a column definition per line, that is followed by
        convention in all yelp *.sql files - however this is NOT a general
        purpose parser/converter.
    """

    def __init__(self, sql_content, base_namespace, default_schema='public'):
        """
        Args:
            sql_content(string): Content of a redshift *.sql file
            base_namespace(string): The base namespace (the namespace will be
                a combination of "{base_namespace}.{schema}"
            default_schema(string): The default schema, for any tables
                encountered which do not specify a schema.
        """
        self.sql_content = sql_content
        self.base_namespace = base_namespace
        self.default_schema = default_schema

    @cached_property
    def avro_record(self):
        """ Get the data pipeline format Avro representation of
        self.sql_content.
        """
        return {
            'type': 'record',
            'namespace': self.namespace,
            'name': self.table,
            'doc': '',
            'pkey': self.pkeys,
            'fields': [
                field_line_converter.avro_field
                for field_line_converter in self.field_line_converters
            ]
        }

    @cached_property
    def namespace(self):
        return '{0}.{1}'.format(self.base_namespace, self.schema)

    @cached_property
    def schema(self):
        m = CREATE_TABLE_REGEX.search(self.create_table_line)
        return m.group(3) if m.group(3) else self.default_schema

    @cached_property
    def table(self):
        m = CREATE_TABLE_REGEX.search(self.create_table_line)
        if m.group(4):
            return m.group(4)
        else:
            raise ValueError("Could not locate the table name")

    @cached_property
    def sql_lines(self):
        return [_sanitize_line(line) for line in self.sql_content.split('\n')]

    @cached_property
    def create_table_line(self):
        for line in self.sql_lines:
            if CREATE_TABLE_REGEX.search(line):
                return line
        raise ValueError("Could not locate a 'CREATE TABLE' statement!")

    @cached_property
    def pkeys(self):
        if self.primary_key_line:
            return [
                pkey.strip() for pkey in
                PRIMARY_KEY_REGEX.search(
                    self.primary_key_line
                ).group(1).split(',')
            ]
        else:
            return []

    @cached_property
    def primary_key_line(self):
        for line in self.sql_lines:
            if self._is_primary_key_line(line):
                return line

    def _is_primary_key_line(self, line):
        return bool(PRIMARY_KEY_REGEX.search(line))

    @cached_property
    def field_line_converters(self):
        return [
            RedshiftFieldLineToAvroFieldConverter(
                field_line=line,
                pkeys=self.pkeys
            )
            for line in self._raw_field_lines
        ]

    @cached_property
    def _raw_field_lines(self):
        in_field_lines = False
        raw_field_lines = []
        for line in self.sql_lines:
            line = _sanitize_line(line)
            if not in_field_lines:
                if self._is_start_line(line):
                    in_field_lines = True
            else:
                if self._is_stop_line(line):
                    break
                elif FIELD_LINE_REGEX.search(line):
                    raw_field_lines.append(line)
        return raw_field_lines

    def _is_start_line(self, line):
        return bool(START_FIELDS_REGEX.search(line))

    def _is_stop_line(self, line):
        return STOP_FIELDS_REGEX.search(line) or self._is_primary_key_line(line)


class RedshiftSQLToAVSCBatch(yelp_batch.batch.Batch):

    notify_emails = ['bam+batch@yelp.com']

    @batch_command_line_options
    def parse_options(self, option_parser):
        opt_group = optparse.OptionGroup(
            option_parser,
            "RedshiftSQLToAVSC Options"
        )
        opt_group.add_option(
            '--glob',
            action='append',
            type='string',
            default=[],
            dest='globs',
            help='[REQUIRED] Either a path to a specific CREATE TABLE redshift'
                 ' *.sql file, or a glob pattern for a directory containing '
                 'such files. (For example: '
                 '"/nail/home/USER/some_dw_redshift_tables/*.sql") '
                 'Note --glob may be provided multiple times.'
        )
        opt_group.add_option(
            '--base-namespace',
            type='string',
            default='yelp_dw_redshift',
            help='[REQUIRED] Base of the namespace. '
                 'Default is "%default"'
        )
        opt_group.add_option(
            '--default-schema',
            type='string',
            default='public',
            help='[REQUIRED] default schema for tables without any specified. '
                 'Default is "%default"'
        )
        opt_group.add_option(
            '--overwrite',
            action="store_true",
            default=False,
            help='Overwrite existing *.avsc files with new output from the '
                 'conversion run. '
                 'Default is "%default"'
        )
        return opt_group

    def run(self):
        """ Primary entry point for the batch
        """
        sql_file_paths = get_file_paths_from_glob_patterns(
            glob_patterns=self.options.globs
        )
        for sql_file_path in sql_file_paths:
            avsc_file_path = sql_file_path.replace('.sql', '.avsc')
            self.log.info(
                'Converting "{0}" to "{1}"'.format(
                    sql_file_path,
                    avsc_file_path
                )
            )
            if os.path.exists(avsc_file_path) and not self.options.overwrite:
                self.log.info(
                    'Skipping "{0}", use "--overwrite" to overwrite existing '
                    '*.avsc files.'.format(
                        avsc_file_path
                    )
                )
                continue
            with open(sql_file_path) as sql_file:
                sql_content = sql_file.read()
            converter = RedshiftSQLToAVSCConverter(
                sql_content=sql_content,
                base_namespace=self.options.base_namespace,
                default_schema=self.options.default_schema
            )
            avro = converter.avro_record
            with open(avsc_file_path, 'w') as avsc_file:
                self.log.info('Writing "{0}"'.format(avsc_file_path))
                json.dump(
                    obj=avro,
                    fp=avsc_file,
                    indent='    ',
                    sort_keys=True
                )

if __name__ == "__main__":
    RedshiftSQLToAVSCBatch().start()
