# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import glob
import inspect
import json
import optparse
import sys
from os import path
from pprint import pformat
from socket import gethostname

import yelp_batch
from yelp_batch.batch import batch_command_line_options

from data_pipeline.config import get_config
from data_pipeline.helpers.frozendict_json_encoder import FrozenDictEncoder
from data_pipeline.tools._glob_util import get_file_paths_from_glob_patterns
from data_pipeline.tools.schema_ref import SchemaRef


class FileBootstrapperBase(object):

    log = get_config().logger

    def __init__(
            self,
            schema_ref,
            file_paths,
            override_metadata,
            file_extension=None
    ):
        """
        Args:
            schema_ref(SchemaRef): SchemaRef to use for looking up metadata
            file_paths(set([str])): A list of file paths to use for bootstrapping
            override_metadata(boolean): If True then existing metadata (such as
                notes, categories, etc) will be overwritten with provided
                schema_ref, otherwise existing metadata will be preserved.
            file_extension(str): Must be specified by subclasses, this should be
                string specifying the file extension the subclass operates on,
                for example 'sql' or 'avsc'
        """
        self.api = get_config().schematizer_client
        self.log = get_config().logger
        self.schema_ref = schema_ref
        self.override_metadata = override_metadata
        self.file_extension = file_extension
        self.file_paths = set([
            file_path
            for file_path in file_paths
            if self.is_correct_file_extension(file_path)
        ])

    def register_file(self, file_path):
        """ Register a file specified and return the schema
        registration results. Subclasses must implement this to handle
        their own file type appropriately.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def is_correct_file_extension(self, file_path):
        return file_path.endswith('.{}'.format(self.file_extension))

    def logged_register_file(self, file_path):
        self.log.info('-' * 40)
        self.log.info('- Registering *.{0} file: {1}'.format(
            self.file_extension,
            file_path
        ))
        return self.register_file(file_path)

    def bootstrap_files(self):
        """ Register all specified files and return a list of the final
        schema results
        """
        self.log.info(
            'Found the following *.{0} files: {1}'.format(
                self.file_extension,
                self.file_paths
            )
        )
        return [self.bootstrap_file(file_path) for file_path in self.file_paths]

    def bootstrap_file(self, file_path):
        """ Bootstrap a file with all metadata from schema_ref and
        return the final swaggerpy schema_result
        """
        schema_result = self.logged_register_file(file_path)
        return self.bootstrap_schema_result(schema_result)

    def bootstrap_schema_result(self, schema_result):
        """ Bootstraps a swaggerpy schema_result with all metadata from
        schema_ref (docs, notes, etc) and return the final schema_result.
        """
        source_ref = self.schema_ref.get_source_ref(
            schema_result.topic.source.name
        )
        if not source_ref:
            return schema_result

        # Updating the docs first, since it requires re-registering with
        # a modified avro schema
        schema_result = self.register_schema_docs(
            schema_result=schema_result,
            source_ref=source_ref
        )

        # Now we can register other attributes which have their own API
        # endpoints, such as categories, file_sources, and notes.
        self.register_schema_note(
            schema_result=schema_result,
            note=self.schema_ref.get_ref_val(source_ref, 'note')
        )
        self.register_category(
            schema_result=schema_result,
            category=self.schema_ref.get_ref_val(source_ref, 'category')
        )
        self.register_file_source(
            schema_result=schema_result,
            display=self.schema_ref.get_ref_val(source_ref, 'file_display'),
            url=self.schema_ref.get_ref_val(source_ref, 'file_url')
        )
        self.register_fields_notes(
            schema_result=schema_result,
            schema_json=json.loads(schema_result.schema),
            fields_ref=source_ref.get('fields', [])
        )
        return schema_result

    def logged_api_call(self, api_method, **kwargs):
        self.log.info(" - Calling {} with args:\n{}".format(
            repr(api_method),
            pformat(kwargs)
        ))
        result = api_method(**kwargs).result()
        self.log.info(" - Result: {}".format(result))
        return result

    def register_schema_docs(self, schema_result, source_ref):
        """ Update the "doc" attributes of the schema and all it's fields,
        returning back the result of the register_schema call.
        """
        source = schema_result.topic.source.name
        schema_json = json.loads(schema_result.schema)
        if self.override_metadata or not schema_json.get('doc'):
            schema_json['doc'] = self.schema_ref.get_ref_val(source_ref, 'doc')
        schema_json = self.update_field_docs(
            schema_json=schema_json,
            fields_ref=source_ref.get('fields', [])
        )
        return self.logged_api_call(
            self.api.schemas.register_schema,
            body={
                'base_schema_id': schema_result.schema_id,
                'schema': json.dumps(schema_json, cls=FrozenDictEncoder),
                'namespace': self.schema_ref.get_source_val(
                    source,
                    'namespace'
                ),
                'source': source,
                'source_owner_email': self.schema_ref.get_source_val(
                    source,
                    'owner_email'
                ),
                'contains_pii': self.schema_ref.get_source_val(
                    source,
                    'contains_pii'
                )
            }
        )

    def update_field_docs(self, schema_json, fields_ref):
        """ Update the "doc" attributes of the fields of a schema.
        """
        field_to_ref_map = {
            field_ref['name']: field_ref for field_ref in fields_ref
        }
        for field_json in schema_json['fields']:
            field_ref = field_to_ref_map.get(field_json['name'])
            if self.override_metadata or not field_json.get('doc'):
                field_json['doc'] = self.schema_ref.get_ref_val(
                    field_ref,
                    'doc'
                )
        return schema_json

    def register_schema_note(self, schema_result, note):
        if note is None:
            return
        if schema_result.note:
            if self.override_metadata:
                self.logged_api_call(
                    self.api.notes.update_note,
                    note_id=schema_result.note.id,
                    body={
                        'note': note,
                        'last_updated_by': self.schema_ref.doc_owner
                    }
                )
        else:
            self.logged_api_call(
                self.api.notes.create_note,
                body={
                    'reference_id': schema_result.schema_id,
                    'reference_type': 'schema',
                    'note': note,
                    'last_updated_by': self.schema_ref.doc_owner
                }
            )

    def register_category(self, schema_result, category):
        if category is None:
            return
        source_result = schema_result.topic.source

        if self.override_metadata or not source_result.category:
            self.logged_api_call(
                self.api.sources.update_category,
                source_id=source_result.source_id,
                body={'category': category}
            )

    def register_file_source(self, schema_result, display, url):
        # TODO (joshszep|DATAPIPE-324) - after file source API is implemented
        pass

    def register_fields_notes(self, schema_result, schema_json, fields_ref):
        field_to_ref_map = {
            field_ref['name']: field_ref for field_ref in fields_ref
        }
        results = self.logged_api_call(
            self.api.schemas.get_schema_elements_by_schema_id,
            schema_id=schema_result.schema_id
        )
        field_to_schema_element_map = {
            self.get_field_name_from_schema_element(result): result
            for result in results
            if result.element_type != 'record'
        }
        for field_json in schema_json['fields']:
            field_name = field_json['name']
            field_ref = field_to_ref_map.get(field_name)
            if not field_ref:
                continue
            self.register_schema_element_note(
                schema_element=field_to_schema_element_map[field_name],
                note=self.schema_ref.get_ref_val(field_ref, 'note')
            )

    def get_field_name_from_schema_element(self, schema_element):
        return schema_element.key.split('|')[-1]

    def register_schema_element_note(self, schema_element, note):
        if note is None:
            return
        if schema_element.note:
            if self.override_metadata:
                return self.logged_api_call(
                    self.api.notes.update_note,
                    note_id=schema_element.note.id,
                    body={
                        'note': note,
                        'last_updated_by': self.schema_ref.doc_owner
                    }
                )
        else:
            return self.logged_api_call(
                self.api.notes.create_note,
                body={
                    'reference_id': schema_element.id,
                    'reference_type': 'schema_element',
                    'note': note,
                    'last_updated_by': self.schema_ref.doc_owner
                }
            )


class AVSCBootstrapper(FileBootstrapperBase):
    """ A file bootstrapper to bootstrap Avro *.avsc json files. Expects all
    files to be formatted following the specification for a data pipeline
    message. See `data_pipeline/schemas/monitoring_message_v1.avsc` for
    an example.
    """

    def __init__(self, schema_ref, file_paths, override_metadata):
        super(AVSCBootstrapper, self).__init__(
            schema_ref, file_paths, override_metadata, file_extension='avsc'
        )

    def register_file(self, file_path):
        with open(file_path) as avsc_file:
            avsc_content = avsc_file.read()
            return self.register_avsc(avsc_content)

    def register_avsc(self, avsc_content):
        self.log.info(
            '\n - Registering avsc content:\n{}'.format(avsc_content)
        )
        avsc = json.loads(avsc_content)
        source = avsc['name']
        return self.logged_api_call(
            self.api.schemas.register_schema,
            body={
                'schema': avsc_content,
                'namespace': avsc['namespace'],
                'source': source,
                'source_owner_email': self.schema_ref.get_source_val(
                    source,
                    'owner_email'
                ),
                'contains_pii': self.schema_ref.get_source_val(
                    source,
                    'contains_pii'
                )
            }
        )


class MySQLBootstrapper(FileBootstrapperBase):
    """ A file bootstrapper to bootstrap MySQL *.sql files. Expects all
    files to contain a CREATE TABLE statement and nothing more.
    """

    def __init__(self, schema_ref, file_paths, override_metadata):
        super(MySQLBootstrapper, self).__init__(
            schema_ref, file_paths, override_metadata, file_extension='sql'
        )

    def register_file(self, file_path):
        with open(file_path) as sql_file:
            return self.register_sql(
                sql_content=sql_file.read(),
                source=self.get_source_from_sql_file_path(file_path)
            )

    def register_sql(self, sql_content, source):
        self.log.info(
            '\n - Registering sql content:\n{}'.format(sql_content)
        )
        return self.logged_api_call(
            self.api.schemas.register_schema_from_mysql_stmts,
            body={
                'new_create_table_stmt': sql_content,
                'namespace': self.schema_ref.get_source_val(
                    source,
                    'namespace'
                ),
                'source': source,
                'source_owner_email': self.schema_ref.get_source_val(
                    source,
                    'owner_email'
                ),
                'contains_pii': self.schema_ref.get_source_val(
                    source,
                    'contains_pii'
                )
            }
        )

    def get_source_from_sql_file_path(self, sql_file_path):
        """ Get a source name for the source by chopping off the `.sql` from
        the filename. Luckily all our tables follow this naming convention.
        """
        return path.split(sql_file_path)[1][:-4]


def is_file_bootstrapper_class(obj):
    return inspect.isclass(obj) and FileBootstrapperBase in obj.__bases__


FILE_BOOTSTRAPPER_CLASSES = [
    obj for _, obj in inspect.getmembers(sys.modules[__name__], is_file_bootstrapper_class)
]


class BootstrapperBatch(yelp_batch.batch.Batch):

    notify_emails = ['bam+batch@yelp.com']

    @batch_command_line_options
    def parse_options(self, option_parser):
        opt_group = optparse.OptionGroup(option_parser, "Bootstrapper Options")
        opt_group.add_option(
            '--glob',
            action='append',
            type='string',
            default=[],
            dest='globs',
            help='[REQUIRED] Either a path to a specific MySQL *.sql file, a '
                 'specific Avro *.avsc file, or a glob pattern for a directory '
                 'containing such files. (For example: '
                 '"/nail/home/USER/pg/yelp-main/schema/yelp_dw/tables/*.sql") '
                 'Note --glob may be provided multiple times.'
        )
        opt_group.add_option(
            '--schema-ref',
            type='string',
            help='File path to load the schema reference json document from.'
                 'See DATAPIPE-259 for more'
                 'information on the schema reference json format.'
        )
        opt_group.add_option(
            '--default-source-owner',
            default='bam@yelp.com',
            type='string',
            help='Owner email of the bootstrapped tables, if none provided by '
                 'the schema-ref. '
                 'Default is %default'
        )
        opt_group.add_option(
            '--default-doc-owner',
            default='bam@yelp.com',
            type='string',
            help='Email of the default owner for the documentation of '
                 'bootstrapped tables, if none is provided by the schema-ref. '
                 'The documentation owner will be known as the last to update '
                 'the descriptions and notes within the bootstrapped tables. '
                 'Default is %default'
        )
        opt_group.add_option(
            '--default-namespace',
            type='string',
            help='Default namespace to register schemas to, if none provided '
                 'by the schema-ref.'
        )
        opt_group.add_option(
            '--default-docstring',
            default='',
            type='string',
            help='Default documentation string for any table descriptions and'
                 'column descriptions which are not provided by the'
                 'schema-ref. '
                 'Default is "%default"'
        )
        opt_group.add_option(
            '--default-contains-pii',
            action="store_true",
            default=False,
            help='Default to mark schemas as containing pii, if not otherwise'
                 'specified by the schema-ref. '
                 'Default is "%default"'
        )
        opt_group.add_option(
            '--default-category',
            type='string',
            default=None,
            help='Default category for schemas, if none provided by the '
                 'schema-ref. If this is not provided the schemas will be '
                 '"[ Uncategorized ]".'
        )
        opt_group.add_option(
            '--http-host',
            default='http://' + gethostname(),
            type='string',
            help='Web hostname. Only used for reporting the links to the doc'
                 'tool view of the schemas which were registered during the '
                 'run. '
                 'Default is "%default"'
        )
        opt_group.add_option(
            '--override-metadata',
            action="store_true",
            default=False,
            help='Override existing metadata for tables (such as notes, '
                 'categories, etc) with provided metadata. By default if'
                 'metadata already exists it will not be changed.'
                 'Default is "%default"'
        )
        return opt_group

    def run(self):
        """ Primary entry point for the batch
        """

        schema_ref = SchemaRef.load_from_file(
            schema_ref_path=self.options.schema_ref,
            defaults={
                'doc_owner': self.options.default_doc_owner,
                'owner_email': self.options.default_source_owner,
                'namespace': self.options.default_namespace,
                'doc': self.options.default_docstring,
                'contains_pii': bool(self.options.default_contains_pii),
                'category': self.options.default_category
            }
        )
        FileBootstrapperBase.log = self.log  # Specify our logger as the logger
        file_paths = get_file_paths_from_glob_patterns(
            self.options.globs
        )
        schema_results = []
        for bootstrapper_cls in FILE_BOOTSTRAPPER_CLASSES:
            bootstrapper = bootstrapper_cls(
                schema_ref=schema_ref,
                file_paths=file_paths,
                override_metadata=self.options.override_metadata
            )
            schema_results += bootstrapper.bootstrap_files()

        self.log_result_urls(schema_results)

    def get_files_from_glob_patterns(self, glob_patterns):
        """ Return a list of files matching the given list of glob patterns
         (for example ["./test.sql", "./other_tables/*.sql"])
        """
        file_paths = []
        for glob_pattern in glob_patterns:
            file_paths += glob.glob(glob_pattern)
        return file_paths

    def log_result_urls(self, schema_results):
        self.log.info("Completed updating the following tables:")
        for schema_result in schema_results:
            self.log.info(
                '{host}/web/#/table?schema={namespace}&table={source}'.format(
                    host='{}:{}'.format(
                        self.options.http_host,
                        get_config().schematizer_port
                    ),
                    namespace=schema_result.topic.source.namespace.name,
                    source=schema_result.topic.source.name
                )
            )


if __name__ == "__main__":
    BootstrapperBatch().start()
