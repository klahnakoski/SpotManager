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
from mo_future import text_type
from mo_kwargs import override
from mo_logs import Log
from mo_logs.strings import expand_template
from mo_math import Math
from mo_math.randoms import Random
from spot.instance_manager import InstanceManager

JRE = "jre-8u131-linux-x64.rpm"
LOCAL_JRE = "resources/" + JRE


class ESSpot(InstanceManager):
    """
    THIS CLASS MUST HAVE AN IMPLEMENTATION FOR the SpotManager TO USE
    """
    @override
    def __init__(self, minimum_utility, kwargs=None):
        InstanceManager.__init__(self, kwargs)
        self.settings = kwargs
        self.minimum_utility = minimum_utility

    def required_utility(self):
        return self.minimum_utility

    def setup(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO SETUP
        utility,    # THE utility OBJECT FOUND IN CONFIG
        please_stop
    ):
        with Connection(host=instance.ip_address, kwargs=self.settings.connect) as conn:
            gigabytes = Math.floor(utility.memory)
            Log.note("setup {{instance}}", instance=instance.id)

            self._install_indexer(instance, conn)
            self._install_es(gigabytes, instance, conn)
            self._install_supervisor(instance, conn)
            self._start_supervisor(conn)

    def teardown(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO TEARDOWN
        please_stop
    ):
        with Connection(host=instance.ip_address, kwargs=self.settings.connect) as conn:
            Log.note("teardown {{instance}}", instance=instance.id)

            # ASK NICELY TO STOP Elasticsearch PROCESS
            conn.sudo("supervisorctl stop es", warn=True)

            # ASK NICELY TO STOP "supervisord" PROCESS
            conn.sudo("ps -ef | grep supervisord | grep -v grep | awk '{print $2}' | xargs kill -SIGINT", warn=True)

            # WAIT FOR SUPERVISOR SHUTDOWN
            pid = True
            while pid:
                pid = conn.sudo("ps -ef | grep supervisord | grep -v grep | awk '{print $2}'")

    def _install_es(self, gigabytes, instance, conn):
        volumes = instance.markup.drives

        if not conn.exists("/usr/local/elasticsearch"):
            with conn.cd("/home/ec2-user/"):
                conn.run("mkdir -p temp")

            if not File(LOCAL_JRE).exists:
                Log.error("Expecting {{file}} on manager to spread to ES instances", file=LOCAL_JRE)
            with conn.cd("/home/ec2-user/temp"):
                conn.run('rm -f '+JRE)
                conn.put("resources/"+JRE, JRE)
                conn.sudo("rpm -i "+JRE)
                conn.sudo("alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000")
                conn.run("export JAVA_HOME=/usr/java/default")

            with conn.cd("/home/ec2-user/"):
                conn.run('wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.7.1.tar.gz')
                conn.run('tar zxfv elasticsearch-1.7.1.tar.gz')
                conn.sudo('mkdir /usr/local/elasticsearch')
                conn.sudo('cp -R elasticsearch-1.7.1/* /usr/local/elasticsearch/')

            with conn.cd('/usr/local/elasticsearch/'):
                # BE SURE TO MATCH THE PLUGLIN WITH ES VERSION
                # https://github.com/elasticsearch/elasticsearch-cloud-aws
                conn.sudo('bin/plugin -install elasticsearch/elasticsearch-cloud-aws/2.7.1')

            # REMOVE THESE FILES, WE WILL REPLACE THEM WITH THE CORRECT VERSIONS AT THE END
            conn.sudo("rm -f /usr/local/elasticsearch/config/elasticsearch.yml")
            conn.sudo("rm -f /usr/local/elasticsearch/bin/elasticsearch.in.sh")

        # MOUNT AND FORMAT THE EBS VOLUMES (list with `lsblk`)
        for i, k in enumerate(volumes):
            if not conn.exists(k.path):
                conn.sudo('sudo umount '+k.device, warn=True)

                conn.sudo('yes | sudo mkfs -t ext4 '+k.device)
                conn.sudo('mkdir '+k.path)
                conn.sudo('sudo mount '+k.device+' '+k.path)

                # ADD TO /etc/fstab SO AROUND AFTER REBOOT
                conn.sudo("sed -i '$ a\\"+k.device+"   "+k.path+"       ext4    defaults,nofail  0   2' /etc/fstab")

        # TEST IT IS WORKING
        conn.sudo('mount -a')

        # INCREASE THE FILE HANDLE LIMITS
        with conn.cd("/home/ec2-user/"):
            File("./results/temp/sysctl.conf").delete()
            conn.get("/etc/sysctl.conf", "./results/temp/sysctl.conf", use_sudo=True)
            lines = File("./results/temp/sysctl.conf").read()
            if lines.find("fs.file-max = 100000") == -1:
                lines += "\nfs.file-max = 100000"
            lines = lines.replace("net.bridge.bridge-nf-call-ip6tables = 0", "")
            lines = lines.replace("net.bridge.bridge-nf-call-iptables = 0", "")
            lines = lines.replace("net.bridge.bridge-nf-call-arptables = 0", "")
            File("./results/temp/sysctl.conf").write(lines)
            conn.put("./results/temp/sysctl.conf", "/etc/sysctl.conf", use_sudo=True)

        conn.sudo("sysctl -p")

        # INCREASE FILE HANDLE PERMISSIONS
        conn.sudo("sed -i '$ a\\root soft nofile 100000' /etc/security/limits.conf")
        conn.sudo("sed -i '$ a\\root hard nofile 100000' /etc/security/limits.conf")
        conn.sudo("sed -i '$ a\\root memlock unlimited' /etc/security/limits.conf")

        conn.sudo("sed -i '$ a\\ec2-user soft nofile 100000' /etc/security/limits.conf")
        conn.sudo("sed -i '$ a\\ec2-user hard nofile 100000' /etc/security/limits.conf")
        conn.sudo("sed -i '$ a\\ec2-user memlock unlimited' /etc/security/limits.conf")

        # EFFECTIVE LOGIN TO LOAD CHANGES TO FILE HANDLES
        # conn.sudo("sudo -i -u ec2-user")

        if not conn.exists("/data1/logs"):
            conn.sudo('mkdir /data1/logs')
            conn.sudo('mkdir /data1/heapdump')

            # INCREASE NUMBER OF FILE HANDLES
            # conn.sudo("sysctl -w fs.file-max=64000")
        # COPY CONFIG FILE TO ES DIR
        if not conn.exists("/usr/local/elasticsearch/config/elasticsearch.yml"):
            yml = File("./examples/config/es_config.yml").read().replace("\r", "")
            yml = expand_template(yml, {
                "id": Random.hex(length=8),
                "data_paths": ",".join("/data"+text_type(i+1) for i, _ in enumerate(volumes))
            })
            File("./results/temp/elasticsearch.yml").write(yml)
            conn.put("./results/temp/elasticsearch.yml", '/usr/local/elasticsearch/config/elasticsearch.yml', use_sudo=True)

        # FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
        # THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
        if not conn.exists("/usr/local/elasticsearch/bin/elasticsearch.in.sh"):
            sh = File("./examples/config/es_run.sh").read().replace("\r", "")
            sh = expand_template(sh, {"memory": text_type(int(gigabytes/2))})
            File("./results/temp/elasticsearch.in.sh").write(sh)
            with conn.cd("/home/ec2-user"):
                conn.put("./results/temp/elasticsearch.in.sh", './temp/elasticsearch.in.sh', use_sudo=True)
                conn.sudo("cp -f ./temp/elasticsearch.in.sh /usr/local/elasticsearch/bin/elasticsearch.in.sh")

    def _install_indexer(self, instance, conn):
        Log.note("Install indexer at {{instance_id}} ({{address}})", instance_id=instance.id, address=instance.ip_address)
        self._install_python(instance, conn)

        if not conn.exists("/home/ec2-user/ActiveData-ETL/"):
            with conn.cd("/home/ec2-user"):
                conn.sudo("yum -y install git")
                conn.run("git clone https://github.com/klahnakoski/ActiveData-ETL.git")

        with conn.cd("/home/ec2-user/ActiveData-ETL/"):
            conn.run("git checkout push-to-es")
            conn.sudo("yum -y install gcc")  # REQUIRED FOR psutil
            conn.sudo("pip install -r requirements.txt")

        conn.put("~/private_active_data_etl.json", "/home/ec2-user/private.json")

    def _install_python(self, instance, conn):
        Log.note("Install Python at {{instance_id}} ({{address}})", instance_id=instance.id, address=instance.ip_address)
        if conn.exists("/usr/bin/pip"):
            pip_version = conn.sudo("pip --version", warn=True)
        else:
            pip_version = ""

        if not pip_version.startswith("pip 9."):
            conn.sudo("yum -y install python27")
            conn.sudo("easy_install pip")
            conn.sudo("rm -f /usr/bin/pip", warn=True)
            conn.sudo("ln -s /usr/local/bin/pip /usr/bin/pip")
            conn.sudo("pip install --upgrade pip")

    def _install_supervisor(self, instance, conn):
        Log.note("Install Supervisor-plus-Cron at {{instance_id}} ({{address}})", instance_id=instance.id, address=instance.ip_address)
        # REQUIRED FOR Python SSH
        self._install_lib("libffi-devel", conn)
        self._install_lib("openssl-devel", conn)
        self._install_lib('"Development tools"', install="groupinstall", conn=conn)

        self._install_python(instance, conn)
        conn.sudo("pip install pyopenssl")
        conn.sudo("pip install ndg-httpsclient")
        conn.sudo("pip install pyasn1")
        conn.sudo("pip install fabric==1.10.2")
        conn.sudo("pip install requests")

        conn.sudo("pip install supervisor-plus-cron")

    def _install_lib(self, lib_name, install="install", conn=None):
        """
        :param lib_name:
        :param install: use 'groupinstall' if you wish
        :return:
        """
        result = conn.sudo("yum "+install+" -y "+lib_name, warn=True)
        if result.return_code != 0 and "already installed and latest version" not in result:
            Log.error("problem with install of {{lib}}", lib=lib_name)

    def _start_supervisor(self, conn):
        conn.put("./examples/config/es_supervisor.conf", "/etc/supervisord.conf", use_sudo=True)

        # START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
        conn.sudo("supervisord -c /etc/supervisord.conf", warn=True)

        conn.sudo("supervisorctl reread")
        conn.sudo("supervisorctl update")
