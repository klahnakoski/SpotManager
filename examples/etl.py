# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from mo_fabric import Connection
from mo_files import File
from mo_kwargs import override
from mo_logs import Log, constants, startup
from mo_logs.strings import between
from mo_math import Math
from mo_times import Date, Timer
from pyLibrary import aws
from spot.instance_manager import InstanceManager


class ETL(InstanceManager):
    @override
    def __init__(
        self,
        work_queue,  # SETTINGS FOR AWS QUEUE
        connect,  # SETTINGS FOR Fabric `env` TO CONNECT TO INSTANCE
        minimum_utility,
        kwargs=None
    ):
        InstanceManager.__init__(self, kwargs)
        self.settings = kwargs

    def required_utility(self, current_utility=None):
        queue = aws.Queue(self.settings.work_queue)
        pending = len(queue)

        tod_minimum = None
        if Date.now().dow not in [6, 7] and Date.now().hour not in [4, 5, 6, 7, 8, 9, 10, 11]:
            tod_minimum = 100
        minimum = max(self.settings.minimum_utility, tod_minimum)

        return max(
            Math.ceiling(pending / 20),   # ENSURE THERE IS PLENTY OF WORK BEFORE MACHINE IS DEPLOYED
            int((current_utility - minimum) / 2) + minimum   # EXPONENTIAL DECAY TO MINIMUM
        )

    def setup(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO SETUP
        utility,    # THE utility OBJECT FOUND IN CONFIG
        please_stop
    ):
        if not self.settings.setup_timeout:
            Log.error("expecting instance.setup_timeout to prevent setup from locking")

        Log.note("setup {{instance}}", instance=instance.id)
        with Connection(host=instance.ip_address, kwargs=self.settings.connect) as c:
            cpu_count = int(round(utility.cpu))

            self._update_ubuntu_packages(c)
            self._setup_etl_code(c)
            self._add_private_file(c)
            self._setup_etl_supervisor(c, cpu_count)

    def teardown(self, instance, please_stop):
        with Connection(host=instance.ip_address, kwargs=self.settings.connect) as conn:
            Log.note("teardown {{instance}}", instance=instance.id)
            conn.sudo("supervisorctl stop all", warn=True)

    def _update_ubuntu_packages(self, conn):
        conn.sudo("apt-get clean")
        conn.sudo("dpkg --configure -a")
        conn.sudo("apt-get clean")
        conn.sudo("apt-get update")

    def _setup_etl_code(self, conn):
        conn.sudo("apt-get install -y python2.7")

        if not conn.exists("/usr/local/bin/pip"):
            conn.run("mkdir -p /home/ubuntu/temp")

            with conn.cd("/home/ubuntu/temp"):
                # INSTALL FROM CLEAN DIRECTORY
                conn.run("wget https://bootstrap.pypa.io/get-pip.py")
                conn.sudo("rm -fr ~/.cache/pip")  # JUST IN CASE THE DIRECTORY WAS MADE
                conn.sudo("python2.7 get-pip.py")

        if not conn.exists("/home/ubuntu/ActiveData-ETL/README.md"):
            with conn.cd("/home/ubuntu"):
                conn.sudo("apt-get -yf install git-core")
                conn.run('rm -fr /home/ubuntu/ActiveData-ETL')
                conn.run("git clone https://github.com/klahnakoski/ActiveData-ETL.git")
                conn.run("mkdir -p /home/ubuntu/ActiveData-ETL/results/logs")

        with conn.cd("/home/ubuntu/ActiveData-ETL"):
            conn.run("git checkout etl")

            # pip install -r requirements.txt HAS TROUBLE IMPORTING SOME LIBS
            conn.sudo("rm -fr ~/.cache/pip")  # JUST IN CASE THE DIRECTORY WAS MADE
            conn.sudo("pip install future")
            conn.sudo("pip install BeautifulSoup")
            conn.sudo("pip install MozillaPulse")
            conn.sudo("pip install boto")
            conn.sudo("pip install requests")
            conn.sudo("pip install taskcluster")
            conn.sudo("apt-get install -y python-dev")  # REQUIRED FOR psutil
            conn.sudo("apt-get install -y build-essential")  # REQUIRED FOR psutil
            conn.sudo("pip install psutil")
            conn.sudo("pip install pympler")
            conn.sudo("pip install -r requirements.txt")

        Log.note("8")
        conn.sudo("apt-get -y install python-psycopg2")

    def _setup_etl_supervisor(self, conn, cpu_count):
        # INSTALL supervsor
        conn.sudo("apt-get install -y supervisor")
        # with fabric_settings(warn=True:
        conn.sudo("service supervisor start")

        # READ LOCAL CONFIG FILE, ALTER IT FOR THIS MACHINE RESOURCES, AND PUSH TO REMOTE
        conf_file = File("./examples/config/etl_supervisor.conf")
        content = conf_file.read_bytes()
        find = between(content, "numprocs=", "\n")
        content = content.replace("numprocs=" + find + "\n", "numprocs=" + str(cpu_count) + "\n")
        File("./temp/etl_supervisor.conf.alt").write_bytes(content)
        conn.sudo("rm -f /etc/supervisor/conf.d/etl_supervisor.conf")
        conn.put("./temp/etl_supervisor.conf.alt", '/etc/supervisor/conf.d/etl_supervisor.conf', use_sudo=True)
        conn.run("mkdir -p /home/ubuntu/ActiveData-ETL/results/logs")

        # POKE supervisor TO NOTICE THE CHANGE
        conn.sudo("supervisorctl reread")
        conn.sudo("supervisorctl update")

    def _add_private_file(self, conn):
        conn.run('rm -f /home/ubuntu/private.json')
        conn.put('~/private_active_data_etl.json', '/home/ubuntu/private.json')
        with conn.cd("/home/ubuntu"):
            conn.run("chmod o-r pr"
                     "ivate.json")


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)
        ETL(settings).setup(settings.instance, settings.utility)
    except Exception as e:
        Log.warning("Problem with setup of ETL", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
