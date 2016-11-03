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

import yaml

from data_pipeline.config import get_config


class Team(object):
    """Contains some helper methods for dealing with the data pipeline team
    configuration.  Eventually this class should encapsulate information about
    each team.  That's overkill for the current use-case, so it's not
    implemented yet.

    """

    @classmethod
    def config(cls):
        """Loads and decodes the
        :attr:`data_pipeline.config.Config.data_pipeline_teams_config_file_path`.

        TODO(justinc|DATAPIPE-348): Cache team config, dealing with invalidation
        when configuration changes.

        Returns:
            dict: team configuration
        """
        config_path = get_config().data_pipeline_teams_config_file_path
        return yaml.load(open(config_path).read())

    @classmethod
    def team_names(cls):
        """Lists all data pipeline teams

        Returns:
            list of str: all valid data pipeline team names
        """
        return cls.config()['teams'].keys()

    @classmethod
    def exists(cls, team_name):
        """Determines if a team exists, by name.

        Returns:
            bool: True if team_name exists for a valid team, false otherwise
        """
        return team_name in cls.team_names()
