# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import glob
import optparse
from os import path
from pprint import pformat

import simplejson as json
import yelp_batch
from cached_property import cached_property
from yelp_batch.batch import batch_command_line_options
from yelp_batch.batch import batch_configure

from data_pipeline.config import get_config


class Bootstrapper(yelp_batch.batch.Batch):

    notify_emails = ['bam+batch@yelp.com']

    @batch_command_line_options
    def parse_options(self, option_parser):
        # Add a new option group and some options for your batch
        opt_group = optparse.OptionGroup(option_parser, "Bootstrapper Options")
        opt_group.add_option(
            '--sql',
            action='append',
            type='string',
            default=[],
            dest='sql_globs',
            help='Either a path to a specific MySQL *.sql file, or a glob '
                 'pattern for a directory of mysql *.sql files. (For example: '
                 '"/nail/home/USER/pg/yelp-main/schema/yelp_dw/tables/*.sql") '
                 'Note that this can be provided multiple times.'
        )
        opt_group.add_option(
            '--avsc',
            action='append',
            type='string',
            default=[],
            dest='avsc_globs',
            help='Either a path to a specific avro *.avsc file, or a glob '
                 'pattern for a directory of avro *.avsc files. (For example: '
                 '"/nail/home/USERNAME/my_avro_schemas/*.avsc")'
                 'Note that this can be provided multiple times.'
        )
        opt_group.add_option(
            '--schema-ref',
            type='string',
            help='File path to load the schema reference json document from.'
                 'See https://jira.yelpcorp.com/browse/DATAPIPE-259 for more'
                 'information on the schema reference json format.'
        )
        opt_group.add_option(
            '--source-owner',
            default='bam@yelp.com',
            type='string',
            help='Owner email of the bootstrapped tables, if none provided by '
                 'the schema-ref. '
                 'Default is %default'
        )
        opt_group.add_option(
            '--doc-owner',
            default='bam@yelp.com',
            type='string',
            help='Email of the default owner for the documentation of '
                 'bootstrapped tables, if none is provided by the schema-ref. '
                 'The documentation owner will be known as the last to update '
                 'the descriptions and notes within the bootstrapped tables. '
                 'Default is %default'
        )
        opt_group.add_option(
            '--namespace',
            type='string',
            help='Default namespace to register schemas to, if none provided '
                 'by the schema-ref.'
        )
        opt_group.add_option(
            '--doc-default',
            default='',
            type='string',
            help='Default documentation string for any table descriptions and'
                 'column descriptions which are not provided by the'
                 'schema-ref'
                 'Default is "%default"'
        )
        return opt_group

    @batch_configure
    def configure(self):
        pass

    @property
    def api(self):
        """ Swaggerpy schematizer client api object
        """
        return get_config().schematizer_client

    @cached_property
    def schema_ref(self):
        """ If a schema ref is specified, load, parse, and return it. Otherwise
        return an empty dictionary. See DATAPIPE-259 for more information on the
        schema ref format.
        """
        schema_ref = {}
        if self.options.schema_ref:
            with open(self.options.schema_ref) as schema_ref_file:
                schema_ref = json.load(schema_ref_file)
        return schema_ref

    @cached_property
    def source_to_ref_map(self):
        """ A dictionary that maps source name to schema ref node. If
        no schema ref is loaded, this will return an empty dictionary.
        """
        source_to_ref_map = {
            ref['source']: ref for ref in self.schema_ref.get('docs', [])
        }
        return source_to_ref_map

    @cached_property
    def doc_owner(self):
        """ The documentation owner as defined by the schema_ref, falling back
        to the specified default if none was provided.
        """
        return self.schema_ref.get('doc_owner', self.options.doc_owner)

    @cached_property
    def doc_default(self):
        """ The default documentation string if none was provided.
        """
        return self.options.doc_default

    @cached_property
    def sql_file_paths(self):
        """ A list of file paths to *.sql files which are to be bootstrapped
        into the schematizer. Possible to be an empty list.
        """
        return self.resolve_glob_patterns(self.options.sql_globs)

    @cached_property
    def avsc_file_paths(self):
        """ A list of file paths to *.avsc files which are to be bootstrapped
        into the schematizer. Possible to be an empty list.
        """
        return self.resolve_glob_patterns(self.options.avsc_globs)

    def resolve_glob_patterns(self, glob_patterns):
        file_paths = []
        for glob_pattern in glob_patterns:
            file_paths += glob.glob(glob_pattern)
        return file_paths

    def get_source_owner_for_source(self, source):
        ref = self.source_to_ref_map.get(source, None)
        if ref:
            return ref.get('owner_email', self.options.source_owner)
        else:
            return self.options.source_owner

    def get_namespace_for_source(self, source):
        ref = self.source_to_ref_map.get(source, None)
        if ref:
            return ref.get('namespace', self.options.namespace)
        else:
            return self.options.namespace

    def register_avsc_files(self):
        """ Register all *.avsc files specified and return a dictionary mapping
        the source names to the schema registration results.
        """
        self.log.info(
            'Found the following *.avsc files: {0}'.format(self.avsc_file_paths)
        )
        source_to_schema_result_map = {}
        for avsc_file_path in self.avsc_file_paths:
            self.log.info('-' * 80)
            self.log.info('- Registering file: {0}'.format(avsc_file_path))
            with open(avsc_file_path) as avsc_file:
                avsc_content = avsc_file.read()
                avsc = json.loads(avsc_content)
                source = avsc['name']
                namespace = avsc['namespace']
                resp = self.register_avsc(
                    avsc_content=avsc_content,
                    namespace=namespace,
                    source=source,
                    source_owner=self.get_source_owner_for_source(source)
                )
                source_to_schema_result_map[source] = resp
        return source_to_schema_result_map

    def register_avsc(self, avsc_content, namespace, source, source_owner):
        self.log.info(
            '\n - Registering avsc content:\n{}'.format(avsc_content)
        )
        return self.logged_api_call(
            self.api.schemas.register_schema,
            body={
                'schema': avsc_content,
                'namespace': namespace,
                'source': source,
                'source_owner_email': source_owner
            }
        )

    def register_sql_files(self):
        """ Register all *.sql files specified and return a dictionary mapping
        the source names to the schema registration results.
        """
        self.log.info(
            'Found the following *.sql files: {0}'.format(self.sql_file_paths)
        )
        source_to_schema_result_map = {
            self.get_source_from_sql_file_path(sql_file_path):
                self.register_sql_file(sql_file_path)
            for sql_file_path in self.sql_file_paths
        }
        return source_to_schema_result_map

    def get_source_from_sql_file_path(self, sql_file_path):
        """ Get a source name for the source by chopping off the `.sql` from
        the filename. Luckily all our tables follow this naming convention.
        """
        return path.split(sql_file_path)[1][:-4]

    def register_sql_file(self, sql_file_path):
        self.log.info('-' * 80)
        self.log.info('- Registering file: {}'.format(sql_file_path))
        with open(sql_file_path) as sql_file:
            source = self.get_source_from_sql_file_path(sql_file_path)
            resp = self.register_sql(
                sql_content=sql_file.read(),
                namespace=self.get_namespace_for_source(source),
                source=source,
                source_owner=self.get_source_owner_for_source(source)
            )
        return resp

    def register_sql(self, sql_content, namespace, source, source_owner):
        self.log.info(
            '\n - Registering sql content:\n{0}'.format(sql_content)
        )
        return self.logged_api_call(
            self.api.schemas.register_schema_from_mysql_stmts,
            body={
                'new_create_table_stmt': sql_content,
                'namespace': namespace,
                'source': source,
                'source_owner_email': source_owner
            }
        )

    def register_category(self, schema_id, category):
        # TODO (joshszep|DATAPIPE-243) - after category API is implemented
        pass

    def register_file_source(self, schema_id, display, url):
        # TODO (joshszep|DATAPIPE-324) - after file source API is implemented
        pass

    def update_field_docs(self, schema_json, fields_ref):
        field_to_ref_map = {
            field_ref['name']: field_ref for field_ref in fields_ref
        }
        for field_json in schema_json['fields']:
            field_json['doc'] = self.get_doc_for_ref(
                field_to_ref_map.get(field_json['name'])
            )
        return schema_json

    def register_fields_notes(self, schema_id, schema_json, fields_ref):
        field_to_ref_map = {
            field_ref['name']: field_ref for field_ref in fields_ref
        }
        results = self.logged_api_call(
            self.api.schemas.get_schema_elements_by_schema_id,
            schema_id=schema_id
        )
        field_to_schema_element_map = {
            result.key.split('|')[-1]: result for result in results
            if result.element_type != 'record'
        }
        for field_json in schema_json['fields']:
            field_name = field_json['name']
            field_ref = field_to_ref_map.get(field_name)
            if not field_ref:
                continue
            self.upsert_schema_element_note(
                note=field_ref.get('note', ''),
                schema_element=field_to_schema_element_map[field_name]
            )

    def upsert_schema_element_note(self, note, schema_element):
        if schema_element.note:
            return self.logged_api_call(
                self.api.notes.update_note,
                note_id=schema_element.note.id,
                body={
                    'note': note,
                    'last_updated_by': self.doc_owner
                }
            )
        else:
            return self.logged_api_call(
                self.api.notes.create_note,
                body={
                    'reference_id': schema_element.id,
                    'reference_type': 'schema_element',
                    'note': note,
                    'last_updated_by': self.doc_owner
                }
            )

    def upsert_schema_note(self, source, schema_result):
        ref = self.source_to_ref_map.get(source, {})
        note = ref.get('note', '')
        if schema_result.note:
            return self.logged_api_call(
                self.api.notes.update_note,
                note_id=schema_result.note.id,
                body={
                    'note': note,
                    'last_updated_by': self.doc_owner
                }
            )
        else:
            return self.logged_api_call(
                self.api.notes.create_note,
                body={
                    'reference_id': schema_result.schema_id,
                    'reference_type': 'schema',
                    'note': note,
                    'last_updated_by': self.doc_owner
                }
            )

    def logged_api_call(self, api_method, **kwargs):
        self.log.info(" - Calling {0} with args:\n{1}".format(
            repr(api_method),
            pformat(kwargs)
        ))
        result = api_method(**kwargs).result()
        self.log.info(" - Result: {0}".format(result))
        return result

    def get_doc_for_ref(self, ref):
        return ref.get('doc', self.doc_default) if ref else self.doc_default

    def update_source_docs(self, source, schema_result, source_ref):
        """ Update the "doc" attributes of the schema and all it's fields,
        returning back the result of the register_schema call.
        """
        if not source_ref:
            return schema_result
        schema_json = json.loads(schema_result.schema)
        schema_json['doc'] = self.get_doc_for_ref(source_ref)
        schema_json = self.update_field_docs(
            schema_json=schema_json,
            fields_ref=source_ref.get('fields')
        )
        return self.logged_api_call(
            self.api.schemas.register_schema,
            body={
                'base_schema_id': schema_result.schema_id,
                'schema': json.dumps(schema_json),
                'namespace': self.get_namespace_for_source(source),
                'source': source,
                'source_owner_email': self.get_source_owner_for_source(source)
            }
        )

    def run(self):
        source_to_schema_result_map = self.register_sql_files()
        source_to_schema_result_map.update(self.register_avsc_files())

        source_to_source_ids = {}
        for source, schema_result in source_to_schema_result_map.iteritems():
            source_ref = self.source_to_ref_map.get(source, None)
            result = self.update_source_docs(source, schema_result, source_ref)
            source_to_source_ids[source] = result.topic.source.source_id
            if not source_ref:
                continue

            schema_json = json.loads(result.schema)
            schema_id = result.schema_id

            self.upsert_schema_note(source, result)
            self.register_category(
                schema_id=schema_id,
                category=source_ref.get('category')
            )
            self.register_file_source(
                schema_id=schema_id,
                display=source_ref.get('file_display'),
                url=source_ref.get('file_url')
            )
            self.register_fields_notes(
                schema_id=schema_id,
                schema_json=schema_json,
                fields_ref=source_ref.get('fields')
            )
        self.log.info(pformat(source_to_source_ids))


if __name__ == "__main__":
    Bootstrapper().start()
