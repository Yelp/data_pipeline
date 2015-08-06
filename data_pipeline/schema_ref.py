# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson as json
from cached_property import cached_property


class SchemaRef(object):
    """ Object representation of the JSON specification for a schema reference.
    A schema reference contains metadata such as notes, docs, categories, and
    other attributes useful to the Documentation Tool.

    The JSON specification is as follows:
        {
          'doc_source': '', // A string, usually a URL, of the original
                            // source the schema ref was generated from.

          'doc_owner': '',  // This will be the username for 'last updated by'
                            // in all notes - it should be a valid email,
                            // usually a team email such as 'bam@yelp.com'

          'docs': [         // An array of dicts, one for each table
            {
              'namespace': '',  // The namespace of the table, which should
                                // be the DB schema if generating docs
                                // for existing tables.

              'source': '', // The source name, which should be the DB table
                            // if generating docs for existing tables.

              'doc': '',    // This will fill the 'description' for the table

              'note': '',   // This will fill the 'notes' for the table

              'category': '',   // This will fill the 'category' for the table

              'owner_email': '',    // This will fill the 'owner' for the table

              'file_display': '',   // This is used for display name of the
                                    // source_file in the documentation tool.
                                    // Generally should be the path to the model
                                    // in relation to the base of the repo

              'file_url': '',   // URL of the file in opengrok/gitweb - used
                                // for the 'source_file' hyperlink target in the
                                // documentation tool

              'contains_pii': false,    // Boolean true/false of if the table
                                        // contains pii

              'fields': [   // An array of dicts, one for each column
                {
                         'name': '',    // Column name of the field
                         'doc': '',     // 'Description' for the column
                         'note': ''     // 'Notes' for the column
                }
              ]
            },
          ]
        }

    See DATAPIPE-259 for more information.
    """

    @classmethod
    def load_from_file(cls, schema_ref_path, **kwargs):
        """ If a schema ref is specified, load, parse, and return it. Otherwise
        return a SchemaRef built from an empty dictionary.
        """
        schema_ref = {}
        if schema_ref_path:
            with open(schema_ref_path) as schema_ref_file:
                schema_ref = json.load(schema_ref_file)
        return cls(schema_ref=schema_ref, **kwargs)

    def __init__(
            self,
            schema_ref,
            defaults
    ):
        """ Construct a SchemaRef from a parsed schema_ref json object.

        Args:
            schema_ref (dict): The parsed schema_ref json object.
            defaults (dict): A key-value pair of schema_ref attributes to
                default values. The following keys are suggested:
                        'doc_owner'    # string, email of doc owner
                        'owner_email'  # string, email of source owner
                        'namespace'    # string, default namespace
                        'doc'          # string, default docstring
                        'contains_pii' # bool, default contains_pii flag
                        'category'     # string, default category
        """
        self.schema_ref = schema_ref
        self.defaults = defaults

    @cached_property
    def _source_to_ref_map(self):
        """ A dictionary that maps source name to schema ref node. If
        no schema ref is loaded, this will return an empty dictionary.
        """
        return {ref['source']: ref for ref in self.schema_ref.get('docs', [])}

    def get_source_ref(self, source):
        """ Get the source ref for a given source name, or None if there
        is none present.
        """
        return self._source_to_ref_map.get(source)

    @cached_property
    def doc_owner(self):
        """ The documentation owner as defined by the schema_ref, falling back
        to the specified default if none was provided.
        """
        return self.get_ref_val(self.schema_ref, 'doc_owner')

    def get_source_val(self, source, key):
        return self.get_ref_val(self.get_source_ref(source), key)

    def get_ref_val(self, ref, key):
        if ref:
            return ref.get(key, self.defaults.get(key))
        else:
            return self.defaults.get(key)
