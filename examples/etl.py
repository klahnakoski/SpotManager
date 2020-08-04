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

import mo_math
from mo_fabric import Connection
from mo_files import File, TempFile
from mo_kwargs import override
from mo_logs import Log, constants, startup
from mo_logs.strings import between
from mo_times import Date
from pyLibrary import aws
from pyLibrary.aws import sqs
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
        queue = sqs.Queue(self.settings.work_queue)
        pending = len(queue)

        tod_minimum = None
        if Date.now().dow not in [6, 7] and Date.now().hour not in [4, 5, 6, 7, 8, 9, 10, 11]:
            tod_minimum = 101
        minimum = max(self.settings.minimum_utility, tod_minimum)

        if current_utility < pending / 20:
            # INCREASE
            return max(minimum, mo_math.ceiling(pending / 20))   # ENSURE THERE IS PLENTY OF WORK BEFORE MACHINE IS DEPLOYED
        else:
            # DECREASE
            target = max(minimum, min(current_utility, pending*2))
            return target + int((current_utility-target) / 2)

    def setup(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO SETUP
        utility,    # THE utility OBJECT FOUND IN CONFIG
        please_stop=False
    ):
        if not self.settings.setup_timeout:
            Log.error("expecting instance.setup_timeout to prevent setup from locking")

        Log.note("setup {{instance}}", instance=instance.id)
        with Connection(host=instance.ip_address, kwargs=self.settings.connect) as c:
            cpu_count = int(round(utility.cpu))

            _setup_etl_code(c, please_stop)
            _add_private_file(c, please_stop)
            _install_supervisor(c, please_stop, cpu_count)
            _restart_etl_supervisor(c, please_stop, cpu_count)

    def teardown(self, instance, please_stop):
        with Connection(host=instance.ip_address, kwargs=self.settings.connect) as conn:
            Log.note("teardown {{instance}}", instance=instance.id)
            conn.sudo("supervisorctl stop all", warn=True)


def _install_python3(conn, please_stop):
    result = conn.run("python3 --version", warn=True)
    if "Python 3.7" not in result:
        conn.sudo("yum install -y python3")


def _setup_etl_code(conn, please_stop):
    _install_python3(conn, please_stop)
    if not conn.exists("/home/ec2-user/ActiveData-ETL/README.md"):
        with conn.cd("/home/ec2-user"):
            conn.sudo("yum -y install git")
            # conn.sudo("yum -y install gcc python3-devel")  # REQUIRED FOR psutil
            conn.run('rm -fr /home/ec2-user/ActiveData-ETL')
            conn.run("git clone https://github.com/klahnakoski/ActiveData-ETL.git")
            conn.run("mkdir -p /home/ec2-user/logs")

    with conn.cd("/home/ec2-user/ActiveData-ETL"):
        conn.run("git reset --hard HEAD")
        conn.run("git checkout etl")
        conn.run("git pull origin etl")

        conn.sudo("rm -fr ~/.cache/pip")  # JUST IN CASE THE DIRECTORY WAS MADE
        conn.sudo("python3 -m pip install -r requirements.txt")


def _install_supervisor(conn, please_stop, cpu_count):
    conn.sudo("easy_install --upgrade pip")
    conn.sudo("pip install supervisor==4.1.0")


def _restart_etl_supervisor(conn, please_stop, cpu_count):
    # READ LOCAL CONFIG FILE, ALTER IT FOR THIS MACHINE RESOURCES, AND PUSH TO REMOTE
    conf_file = File("./examples/config/etl_supervisor.conf")
    content = conf_file.read_bytes()
    find = between(content, "numprocs=", "\n")
    content = content.replace("numprocs=" + find + "\n", "numprocs=" + str(cpu_count) + "\n")
    with TempFile() as tempfile:
        tempfile.write(content)
        conn.sudo("rm -f /etc/supervisor/conf.d/etl_supervisor.conf")
        conn.put(tempfile.abspath, "/etc/supervisord.conf", use_sudo=True)
    conn.run("mkdir -p /home/ec2-user/logs")

    # START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
    conn.sudo("supervisord -c /etc/supervisord.conf", warn=True)
    conn.sudo("supervisorctl reread")
    conn.sudo("supervisorctl update")


def _add_private_file(conn, please_Stop):
    conn.run('rm -f /home/ec2-user/private.json')
    conn.put('~/private_active_data_etl.json', '/home/ec2-user/private.json')
    with conn.cd("/home/ec2-user"):
        conn.run("chmod go-rw private.json")


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
