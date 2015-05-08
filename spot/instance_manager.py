# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division


class InstanceManager(object):
    """
    THIS CLASS MUST HAVE AN IMPLEMENTATION FOR the SpotManager TO USE
    """


    def __init__(self, settings):
        self.settings = settings

    def required_utility(self):
        raise NotImplementedError()

    def setup(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO SETUP
        utility
    ):
        pass

    def teardown(
        self,
        instance   # THE boto INSTANCE OBJECT FOR THE MACHINE TO TEARDOWN
    ):
        pass

