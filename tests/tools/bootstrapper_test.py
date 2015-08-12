# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
import simplejson as json

from data_pipeline.tools.bootstrapper import AVSCBootstrapper
from data_pipeline.tools.bootstrapper import FileBootstrapperBase
from data_pipeline.tools.bootstrapper import MySQLBootstrapper


class TestFileBootstrapperBase(object):

    @pytest.fixture()
    def mock_schema_result(self):
        mock_schema_result = mock.Mock()
        mock_schema_result.schema = '{"doc":"","fields":' \
                                    '[{"name":"good_field"}]}'
        mock_schema_result.schema_id = 1
        mock_schema_result.topic.source.source = 'good_source'
        return mock_schema_result

    @pytest.fixture
    def mock_schema_json(self, mock_schema_result):
        return json.loads(mock_schema_result.schema)

    @pytest.fixture
    def bootstrapper(self, containers, schema_ref):
        """Note that `containers` is needed to bootstrap the api field
        """
        bootstrapper = FileBootstrapperBase(
            schema_ref=schema_ref,
            file_paths=[],
            override_metadata=True
        )
        bootstrapper.logged_api_call = mock.Mock()
        return bootstrapper

    def test_bootstrap_files_calls_register_file_for_each_file(self, containers):
        file_paths = ['a.test', 'b.test']
        bootstrapper = FileBootstrapperBase(
            schema_ref=None,
            file_paths=file_paths,
            override_metadata=True,
            file_extension='test'
        )
        bootstrapper.register_file = mock.Mock(return_value=None)
        bootstrapper.bootstrap_schema_result = mock.Mock()
        bootstrapper.bootstrap_files()
        assert bootstrapper.register_file.mock_calls == [
            mock.call(file_path) for file_path in file_paths
        ]
        assert bootstrapper.bootstrap_schema_result.mock_calls == [
            mock.call(None) for _ in file_paths
        ]

    def test_bootstrap_schema_result_calls_register_methods(
            self,
            mock_schema_result,
            mock_schema_json,
            bootstrapper,
            good_source_ref
    ):
        bootstrapper.register_schema_docs = mock.Mock(
            side_effect=lambda schema_result, source_ref: schema_result
        )
        bootstrapper.register_schema_note = mock.Mock()
        bootstrapper.register_category = mock.Mock()
        bootstrapper.register_file_source = mock.Mock()
        bootstrapper.register_fields_notes = mock.Mock()

        bootstrapper.bootstrap_schema_result(mock_schema_result)
        assert bootstrapper.register_schema_docs.mock_calls == [
            mock.call(
                schema_result=mock_schema_result,
                source_ref=good_source_ref
            )
        ]
        assert bootstrapper.register_schema_note.mock_calls == [
            mock.call(
                schema_result=mock_schema_result,
                note=good_source_ref['note']
            )
        ]
        assert bootstrapper.register_category.mock_calls == [
            mock.call(
                schema_result=mock_schema_result,
                category=good_source_ref['category']
            )
        ]
        assert bootstrapper.register_file_source.mock_calls == [
            mock.call(
                schema_result=mock_schema_result,
                display=good_source_ref['file_display'],
                url=good_source_ref['file_url']
            )
        ]
        assert bootstrapper.register_fields_notes.mock_calls == [
            mock.call(
                schema_result=mock_schema_result,
                schema_json=mock_schema_json,
                fields_ref=good_source_ref['fields']
            )
        ]

    def test_register_schema_docs_fills_all_docs(
            self,
            schema_ref,
            mock_schema_result,
            bootstrapper,
            good_source_ref
    ):
        bootstrapper.register_schema_docs(
            schema_result=mock_schema_result,
            source_ref=schema_ref.get_source_ref('good_source')
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.schemas.register_schema,
                body={
                    'base_schema_id': 1,
                    'schema': b'{"doc": "Docs for good_source", '
                              b'"fields": [{"doc": "Docs for good_field"'
                              b', "name": "good_field"}]}',
                    'namespace': good_source_ref['namespace'],
                    'source': good_source_ref['source'],
                    'source_owner_email': good_source_ref['owner_email'],
                    'contains_pii': good_source_ref['contains_pii']
                }
            )
        ]

    def test_register_schema_note_updates_note_if_exists(
            self,
            schema_ref,
            mock_schema_result,
            bootstrapper
    ):
        bootstrapper.register_schema_note(
            schema_result=mock_schema_result,
            note='test_note'
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.notes.update_note,
                note_id=mock_schema_result.note.id,
                body={
                    'note': 'test_note',
                    'last_updated_by': schema_ref.doc_owner
                }
            )
        ]

    def test_register_schema_note_does_nothing_note_if_exists_and_no_override(
            self,
            mock_schema_result,
            bootstrapper
    ):
        bootstrapper.override_metadata = False
        bootstrapper.register_schema_note(
            schema_result=mock_schema_result,
            note='test_note'
        )
        assert bootstrapper.logged_api_call.mock_calls == []

    def test_register_schema_creates_note_if_none_exists(
            self,
            schema_ref,
            mock_schema_result,
            bootstrapper
    ):
        mock_schema_result.note = None
        bootstrapper.register_schema_note(
            schema_result=mock_schema_result,
            note='test_note'
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.notes.create_note,
                body={
                    'reference_id': mock_schema_result.schema_id,
                    'reference_type': 'schema',
                    'note': 'test_note',
                    'last_updated_by': schema_ref.doc_owner
                }
            )
        ]

    def test_register_category_updates_category_if_has_note_and_override(
            self,
            mock_schema_result,
            bootstrapper
    ):
        bootstrapper.register_category(
            schema_result=mock_schema_result,
            category='test_category'
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.sources.update_category,
                source_id=mock_schema_result.topic.source.source_id,
                body={'category': 'test_category'}
            )
        ]

    def test_register_category_updates_category_if_no_category_exists(
            self,
            mock_schema_result,
            bootstrapper
    ):
        mock_schema_result.topic.source.category = None
        bootstrapper.register_category(
            schema_result=mock_schema_result,
            category='test_category'
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.sources.update_category,
                source_id=mock_schema_result.topic.source.source_id,
                body={'category': 'test_category'}
            )
        ]

    def test_register_category_does_nothing_if_not_override_and_category_exists(
            self,
            mock_schema_result,
            bootstrapper
    ):
        bootstrapper.override_metadata = False
        bootstrapper.register_category(
            schema_result=mock_schema_result,
            category='test_category'
        )
        assert bootstrapper.logged_api_call.mock_calls == []

    def test_register_fields_notes(
            self,
            mock_schema_result,
            mock_schema_json,
            bootstrapper,
            good_source_ref
    ):
        mock_schema_element_result = mock.Mock()
        mock_schema_element_result.key = 'good_source|good_field'
        bootstrapper.logged_api_call = mock.Mock(
            return_value=[mock_schema_element_result]
        )
        bootstrapper.register_schema_element_note = mock.Mock()

        bootstrapper.register_fields_notes(
            schema_result=mock_schema_result,
            schema_json=mock_schema_json,
            fields_ref=good_source_ref['fields']
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.schemas.get_schema_elements_by_schema_id,
                schema_id=mock_schema_result.schema_id
            )
        ]
        assert bootstrapper.register_schema_element_note.mock_calls == [
            mock.call(
                note=good_source_ref['fields'][0]['note'],
                schema_element=mock_schema_element_result
            )
        ]

    def test_register_schema_element_note_updates_if_has_note_and_override(
            self,
            schema_ref,
            bootstrapper
    ):
        mock_schema_element_result = mock.Mock()
        bootstrapper.register_schema_element_note(
            schema_element=mock_schema_element_result,
            note='test_note'
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.notes.update_note,
                note_id=mock_schema_element_result.note.id,
                body={
                    'note': 'test_note',
                    'last_updated_by': schema_ref.doc_owner
                }
            )
        ]

    def test_register_schema_element_note_does_nothing_if_note_without_override(
            self,
            bootstrapper
    ):
        mock_schema_element_result = mock.Mock()
        bootstrapper.override_metadata = False
        bootstrapper.register_schema_element_note(
            schema_element=mock_schema_element_result,
            note='test_note'
        )
        assert bootstrapper.logged_api_call.mock_calls == []

    def test_register_schema_element_note_creatsif_no_note(
            self,
            schema_ref,
            bootstrapper
    ):
        mock_schema_element_result = mock.Mock()
        mock_schema_element_result.note = None
        bootstrapper.register_schema_element_note(
            schema_element=mock_schema_element_result,
            note='test_note'
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.notes.create_note,
                body={
                    'reference_id': mock_schema_element_result.id,
                    'reference_type': 'schema_element',
                    'note': 'test_note',
                    'last_updated_by': schema_ref.doc_owner
                }
            )
        ]


class TestAVSCBootstrapper(object):

    @pytest.fixture
    def bootstrapper(self, schema_ref):
        bootstrapper = AVSCBootstrapper(
            schema_ref=schema_ref,
            file_paths=['test.avsc', 'test.not_an_avsc'],
            override_metadata=True
        )
        bootstrapper.logged_api_call = mock.Mock()
        return bootstrapper

    def test_only_avsc_remain_in_file_paths(self, bootstrapper):
        assert bootstrapper.file_paths == ['test.avsc']

    def test_register_avsc(self, bootstrapper, example_schema, good_source_ref):
        bootstrapper.register_avsc(avsc_content=example_schema)
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.schemas.register_schema,
                body={
                    'schema': example_schema,
                    'namespace': 'test_namespace',
                    'source': good_source_ref['source'],
                    'source_owner_email': good_source_ref['owner_email'],
                    'contains_pii': good_source_ref['contains_pii']
                }
            )
        ]


class TestMySQLBootstrapper(object):

    @pytest.fixture
    def bootstrapper(self, schema_ref):
        bootstrapper = MySQLBootstrapper(
            schema_ref=schema_ref,
            file_paths=['test.sql', 'test.not_a_sql'],
            override_metadata=True
        )
        bootstrapper.logged_api_call = mock.Mock()
        return bootstrapper

    def test_only_sql_remain_in_file_paths(self, bootstrapper):
        assert bootstrapper.file_paths == ['test.sql']

    def get_source_from_sql_file_path(self, bootstrapper):
        assert bootstrapper.get_source_from_sql_file_path(
            sql_file_path='test.sql'
        ) == 'test'
        assert bootstrapper.get_source_from_sql_file_path(
            sql_file_path='long/complicated/path/test.sql'
        ) == 'test'

    def test_register_sql(self, bootstrapper, good_source_ref):
        mock_sql_content = 'blah blah I am some sql but not really'
        bootstrapper.register_sql(
            sql_content=mock_sql_content,
            source=good_source_ref['source']
        )
        assert bootstrapper.logged_api_call.mock_calls == [
            mock.call(
                bootstrapper.api.schemas.register_schema_from_mysql_stmts,
                body={
                    'new_create_table_stmt': mock_sql_content,
                    'namespace': good_source_ref['namespace'],
                    'source': good_source_ref['source'],
                    'source_owner_email': good_source_ref['owner_email'],
                    'contains_pii': good_source_ref['contains_pii']
                }
            )
        ]
