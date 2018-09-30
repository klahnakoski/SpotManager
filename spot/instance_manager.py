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

    def setup_required(self):
        # Return False here if your AMI is configured to entirely handle
        # running jobs by itself and needs no external work to set it up.
        # SpotManager will entirely ignore instances after placing bids.
        #
        # Note that managed instances MUST still have a Name tag that
        # starts with settings.ec2.instance.name.  This means your
        # instance will need to create its own Name tag.
        #
        # If this returns False, setup() and teardown() will never be
        # called and need not be implemented.
        return True

    def setup(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO SETUP
        utility,
        please_stop
    ):
        pass

    def teardown(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO TEARDOWN
        please_stop
    ):
        pass

