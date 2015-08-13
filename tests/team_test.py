# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from data_pipeline.team import Team


@pytest.mark.usefixtures('configure_teams')
class TestTeam(object):
    def test_team_exists(self):
        assert Team.exists('bam')

    def test_team_does_not_exist(self):
        assert not Team.exists('fake_team')
