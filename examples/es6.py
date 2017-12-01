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

from fabric.api import settings as fabric_settings
from fabric.context_managers import cd, hide
from fabric.contrib import files as fabric_files
from fabric.operations import sudo, run, put, get
from fabric.state import env
from mo_future import text_type

from mo_files import File
from mo_kwargs import override
from mo_logs import Log
from mo_logs.strings import expand_template
from mo_math import Math
from mo_threads import Lock
from spot.instance_manager import InstanceManager

JRE = "jre-8u131-linux-x64.rpm"
LOCAL_JRE = "resources/" + JRE


class ES6Spot(InstanceManager):
    """
    THIS CLASS MUST HAVE AN IMPLEMENTATION FOR the SpotManager TO USE
    """
    @override
    def __init__(self, minimum_utility, kwargs=None):
        self.settings = kwargs
        self.minimum_utility = minimum_utility
        self.conn = None
        self.instance = None
        self.locker = Lock()

    def required_utility(self):
        return self.minimum_utility

    def setup(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO SETUP
        utility     # THE utility OBJECT FOUND IN CONFIG
    ):
        with self.locker:
            self.instance = instance
            gigabytes = Math.floor(utility.memory)
            Log.note("setup {{instance}}", instance=instance.id)
            with hide('output'):
                self._config_fabric(instance)
                self._install_indexer()
                self._install_supervisor()
                self._install_es(gigabytes)
                self._start_supervisor()

    def teardown(
        self,
        instance   # THE boto INSTANCE OBJECT FOR THE MACHINE TO TEARDOWN
    ):
        with self.locker:
            self.instance = instance
            Log.note("teardown {{instance}}", instance=instance.id)
            self._config_fabric(instance)

            # ASK NICELY TO STOP Elasticsearch PROCESS
            with fabric_settings(warn_only=True):
                sudo("supervisorctl stop es")

            # ASK NICELY TO STOP "supervisord" PROCESS
            with fabric_settings(warn_only=True):
                sudo("ps -ef | grep supervisord | grep -v grep | awk '{print $2}' | xargs kill -SIGINT")

            # WAIT FOR SUPERVISOR SHUTDOWN
            pid = True
            while pid:
                with hide('output'):
                    pid = sudo("ps -ef | grep supervisord | grep -v grep | awk '{print $2}'")

    def _config_fabric(self, instance):
        if not instance.ip_address:
            Log.error("Expecting an ip address for {{instance_id}}", instance_id=instance.id)

        for k, v in self.settings.connect.items():
            env[k] = v
        env.host_string = instance.ip_address
        env.abort_exception = Log.error

    def _install_es(self, gigabytes, es_version="6.0.0"):
        volumes = self.instance.markup.drives

        if not fabric_files.exists("/usr/local/elasticsearch/config/elasticsearch.yml"):
            with cd("/home/ec2-user/"):
                run("mkdir -p temp")

            if not File(LOCAL_JRE).exists:
                Log.error("Expecting {{file}} on manager to spread to ES instances", file=LOCAL_JRE)
            with cd("/home/ec2-user/temp"):
                run('rm -f '+JRE)
                put("resources/"+JRE, JRE)
                sudo("rpm -i "+JRE)
                sudo("alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000")
                run("export JAVA_HOME=/usr/java/default")

            with cd("/home/ec2-user/"):
                run('wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-'+es_version+'.tar.gz')
                run('tar zxfv elasticsearch-'+es_version+'.tar.gz')
                sudo('mkdir /usr/local/elasticsearch')
                sudo('cp -R elasticsearch-'+es_version+'/* /usr/local/elasticsearch/')

            with cd('/usr/local/elasticsearch/'):
                # BE SURE TO MATCH THE PLUGLIN WITH ES VERSION
                # https://github.com/elasticsearch/elasticsearch-cloud-aws
                sudo('sudo bin/elasticsearch-plugin install -b discovery-ec2')

            # REMOVE THESE FILES, WE WILL REPLACE THEM WITH THE CORRECT VERSIONS AT THE END
            sudo("rm -f /usr/local/elasticsearch/config/elasticsearch.yml")
            sudo("rm -f /usr/local/elasticsearch/config/jvm.options")
            sudo("rm -f /usr/local/elasticsearch/config/log4j2.properties")

        self.conn = self.instance.connection

        # MOUNT AND FORMAT THE EBS VOLUMES (list with `lsblk`)
        for i, k in enumerate(volumes):
            if not fabric_files.exists(k.path):
                sudo('yes | sudo mkfs -t ext4 '+k.device)
                sudo('mkdir '+k.path)
                sudo('sudo mount '+k.device+' '+k.path)
                sudo('chown -R ec2-user:ec2-user '+k.path)

                #ADD TO /etc/fstab SO AROUND AFTER REBOOT
                sudo("sed -i '$ a\\"+k.device+"   "+k.path+"       ext4    defaults,nofail  0   2' /etc/fstab")

        # TEST IT IS WORKING
        sudo('mount -a')

        # INCREASE THE FILE HANDLE LIMITS
        with cd("/home/ec2-user/"):
            File("./results/temp/sysctl.conf").delete()
            get("/etc/sysctl.conf", "./results/temp/sysctl.conf", use_sudo=True)
            lines = File("./results/temp/sysctl.conf").read()
            if lines.find("fs.file-max = 100000") == -1:
                lines += "\nfs.file-max = 100000"
            lines = lines.replace("net.bridge.bridge-nf-call-ip6tables = 0", "")
            lines = lines.replace("net.bridge.bridge-nf-call-iptables = 0", "")
            lines = lines.replace("net.bridge.bridge-nf-call-arptables = 0", "")
            File("./results/temp/sysctl.conf").write(lines)
            put("./results/temp/sysctl.conf", "/etc/sysctl.conf", use_sudo=True)

        sudo("sudo sed -i '$ a\\vm.max_map_count = 262144' /etc/sysctl.conf")

        sudo("sysctl -p")

        # INCREASE FILE HANDLE PERMISSIONS
        sudo("sed -i '$ a\\ec2-user soft nofile 65536' /etc/security/limits.conf")
        sudo("sed -i '$ a\\ec2-user hard nofile 65536' /etc/security/limits.conf")
        sudo("sed -i '$ a\\ec2-user soft memlock unlimited' /etc/security/limits.conf")
        sudo("sed -i '$ a\\ec2-user hard memlock unlimited' /etc/security/limits.conf")

        # EFFECTIVE LOGIN TO LOAD CHANGES TO FILE HANDLES
        # sudo("sudo -i -u ec2-user")

        if not fabric_files.exists("/data1/logs"):
            run('mkdir /data1/logs')
            run('mkdir /data1/heapdump')

        # COPY CONFIG FILES TO ES DIR
        if not fabric_files.exists("/usr/local/elasticsearch/config/elasticsearch.yml"):
            put("./examples/config/es6_log4j2.properties", '/usr/local/elasticsearch/config/log4j2.properties', use_sudo=True)

            jvm = File("./examples/config/es6_jvm.options").read().replace('\r', '')
            jvm = expand_template(jvm, {"memory": int(gigabytes/2)})
            File("./results/temp/jvm.options").write(jvm)
            put("./results/temp/jvm.options", '/usr/local/elasticsearch/config/jvm.options', use_sudo=True)

            yml = File("./examples/config/es6_config.yml").read().replace("\r", "")
            yml = expand_template(yml, {
                "id": self.instance.ip_address,
                "data_paths": ",".join("/data" + text_type(i + 1) for i, _ in enumerate(volumes))
            })
            File("./results/temp/elasticsearch.yml").write(yml)
            put("./results/temp/elasticsearch.yml", '/usr/local/elasticsearch/config/elasticsearch.yml', use_sudo=True)

        sudo("chown -R ec2-user:ec2-user /usr/local/elasticsearch")

    def _install_indexer(self):
        Log.note("Install indexer at {{instance_id}} ({{address}})", instance_id=self.instance.id, address=self.instance.ip_address)
        self._install_python()

        if not fabric_files.exists("/home/ec2-user/ActiveData-ETL/"):
            with cd("/home/ec2-user"):
                sudo("yum -y install git")
                run("git clone https://github.com/klahnakoski/ActiveData-ETL.git")

        with cd("/home/ec2-user/ActiveData-ETL/"):
            run("git checkout push-to-es6")
            sudo("yum -y install gcc")  # REQUIRED FOR psutil
            sudo("pip install -r requirements.txt")

        put("~/private_active_data_etl.json", "/home/ec2-user/private.json")

    def _install_python(self):
        Log.note("Install Python at {{instance_id}} ({{address}})", instance_id=self.instance.id, address=self.instance.ip_address)
        if fabric_files.exists("/usr/bin/pip"):
            with fabric_settings(warn_only=True):
                pip_version = sudo("pip --version")
        else:
            pip_version = ""

        if not pip_version.startswith("pip 9."):
            sudo("yum -y install python27")
            sudo("easy_install pip")
            with fabric_settings(warn_only=True):
                sudo("rm -f /usr/bin/pip")
            sudo("ln -s /usr/local/bin/pip /usr/bin/pip")
            sudo("pip install --upgrade pip")

    def _install_supervisor(self):
        Log.note("Install Supervisor-plus-Cron at {{instance_id}} ({{address}})", instance_id=self.instance.id, address=self.instance.ip_address)
        # REQUIRED FOR Python SSH
        self._install_lib("libffi-devel")
        self._install_lib("openssl-devel")
        self._install_lib('"Development tools"', install="groupinstall")

        self._install_python()
        sudo("pip install pyopenssl")
        sudo("pip install ndg-httpsclient")
        sudo("pip install pyasn1")
        sudo("pip install fabric==1.10.2")
        sudo("pip install requests")

        sudo("pip install supervisor-plus-cron")

    def _install_lib(self, lib_name, install="install"):
        """
        :param lib_name:
        :param install: use 'groupinstall' if you wish
        :return:
        """
        with fabric_settings(warn_only=True):
            result = sudo("yum "+install+" -y "+lib_name)
            if result.return_code != 0 and result.find("already installed and latest version")==-1:
                Log.error("problem with install of {{lib}}", lib=lib_name)

    def _start_supervisor(self):
        put("./examples/config/es_supervisor.conf", "/etc/supervisord.conf", use_sudo=True)

        # START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
        with fabric_settings(warn_only=True):
            sudo("supervisord -c /etc/supervisord.conf")

        sudo("supervisorctl reread")
        sudo("supervisorctl update")
