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
"""
This script will create a JSON version of the Google Spreadsheet schema reference.

Google Spreadsheet:
https://docs.google.com/spreadsheets/d/1ZIE8UdMadTgBpcELOnGLNJIhOLXBsij0_f5BiwlAQss/edit#gid=8

Usage:
    1. download sheets as csvs from the Google Spreadsheet into the same directory as this script
        - ref_cols.csv          Sheet 1 ("DW Schema Reference")
        - ref_tables.csv        Sheet 2 ("DW Tables")
    2. download BAM's listings of table owners into the same directory as this script
        - ref_owners.csv        Original Version
        - ref_owners_new.csv    Updated Version
    3. run the script and the output will be in schema_ref.json
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import json

from data_pipeline.helpers.frozendict_json_encoder import FrozenDictEncoder


def _read_rows_from_file(file_name):

    rows = []
    with open(file_name, 'rb') as file:
        reader = csv.reader(file)
        for row in reader:
            rows.append(row)
        return rows


def _parse_col_row(row):

    _, _, col_name, pos, _, nullable, write_once, data_type, _, _, _, _, description, _, notes = row

    if nullable == 'NO':
        data_type += ' not null'
    if write_once == 'YES':
        data_type += ' write once'

    if notes.strip() == '0':
        notes = ''

    return {
        'name': col_name,
        'doc': description,
        'note': notes,
    }


if __name__ == '__main__':

    owners_rows = _read_rows_from_file('ref_owners.csv')
    owners_new_rows = _read_rows_from_file('ref_owners_new.csv')
    tables_rows = _read_rows_from_file('ref_tables.csv')
    cols_rows = _read_rows_from_file('ref_cols.csv')

    tables_rows = tables_rows[1:]

    output = {
        'doc_source': 'https://docs.google.com/spreadsheets/d/1ZIE8UdMadTgBpcELOnGLNJIhOLXBsij0_f5BiwlAQss/edit#gid=11',
        'doc_owner': 'bam@yelp.com',
        'docs': []
    }

    for row in tables_rows:

        schema, name, category, description, _, _, notes, _ = row
        table_output = {
            'namespace': schema,
            'source': name,
            'doc': description,
            'note': notes,
            'category': category,
            'fields': []
        }

        owner_row = filter(lambda row: row[3] == name, owners_rows)
        owner_new_row = filter(lambda row: row[3] == name, owners_new_rows)

        try:
            _, source_path, _, _, _, owner = owner_row.pop()
            owner = owner.split(',')[0]

            table_output['owner_email'] = owner
            table_output['file_display'] = source_path
            table_output['file_url'] = 'https://opengrok.yelpcorp.com/xref/yelp-main/' + source_path

        except IndexError:

            if len(owner_new_row) > 0:

                _, source_path, _, _, _, owner = owner_new_row.pop()
                owner = owner.split(',')[0]

                table_output['owner_email'] = owner
                table_output['file_display'] = source_path
                table_output['file_url'] = 'https://opengrok.yelpcorp.com/xref/yelp-main/' + source_path

            else:

                table_output['owner_email'] = ''
                table_output['file_display'] = ''
                table_output['file_url'] = ''

        col_rows = filter(lambda row: row[0] == schema and row[1] == name, cols_rows)

        for row in col_rows:
            table_output['fields'].append(_parse_col_row(row))

        output['docs'].append(table_output)

    with open('schema_ref.json', 'wb') as outfile:
        outfile.write(json.dumps(output, cls=FrozenDictEncoder))
        outfile.close()
