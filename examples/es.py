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

from fabric.api import settings as fabric_settings
from fabric.context_managers import cd, hide
from fabric.contrib import files as fabric_files
from fabric.operations import sudo, run, put, get
from fabric.state import env

from pyLibrary.debugs.logs import Log
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.maths.randoms import Random
from pyLibrary.meta import use_settings
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Lock
from spot.instance_manager import InstanceManager


class ESSpot(InstanceManager):
    """
    THIS CLASS MUST HAVE AN IMPLEMENTATION FOR the SpotManager TO USE
    """
    @use_settings
    def __init__(self, settings):
        self.settings = settings
        self.conn = None
        self.instance = None
        self.locker = Lock()

    def required_utility(self):
        return self.settings.minimum_utility

    def setup(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO SETUP
        utility
    ):
        with self.locker:
            self.instance = instance
            gigabytes = Math.floor(utility, 15)
            Log.note("setup {{instance}}", instance=instance.id)
            with hide('output'):
                self._config_fabric(instance)
                self._install_es(gigabytes)
                self._install_indexer()
                self._install_supervisor()
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

    def _set_mtu(self, mtu=1500):
        # SET RIGHT NOW
        sudo("ifconfig eth0 mtu "+unicode(mtu))

        # DESPITE THE FILE CHANGE, THE MTU VALUE DOES NOT STICK
        local_file = File("./results/temp/ifcfg-eth0")
        local_file.delete()
        get("/etc/sysconfig/network-scripts/ifcfg-eth0", "./results/temp/ifcfg-eth0", use_sudo=True)
        lines = local_file.read()
        if lines.find("MTU=1500") == -1:
            lines += "\nMTU=1500"
        local_file.write(lines)
        put("./results/temp/ifcfg-eth0", "/etc/sysconfig/network-scripts/ifcfg-eth0", use_sudo=True)

    def _install_es(self, gigabytes):
        volumes = self.instance.markup.drives

        if not fabric_files.exists("/usr/local/elasticsearch"):
            with cd("/home/ec2-user/"):
                run("mkdir -p temp")

            with cd("/home/ec2-user/temp"):
                run('rm -f jdk-8u5-linux-x64.rpm')
                run('wget -c --no-cookies --no-check-certificate --header "Cookie: s_cc=true; s_nr=1425654197863; s_sq=%5B%5BB%5D%5D; oraclelicense=accept-securebackup-cookie; gpw_e24=http%3A%2F%2Fwww.oracle.com%2Ftechnetwork%2Fjava%2Fjavase%2Fdownloads%2Fjre8-downloads-2133155.html" "http://download.oracle.com/otn-pub/java/jdk/8u40-b25/jre-8u40-linux-x64.rpm" --output-document="jdk-8u5-linux-x64.rpm"')
                sudo("rpm -i jdk-8u5-linux-x64.rpm")
                sudo("alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000")
                run("export JAVA_HOME=/usr/java/default")

            with cd("/home/ec2-user/"):
                run('wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.7.1.tar.gz')
                run('tar zxfv elasticsearch-1.7.1.tar.gz')
                sudo('mkdir /usr/local/elasticsearch')
                sudo('cp -R elasticsearch-1.7.1/* /usr/local/elasticsearch/')

            with cd('/usr/local/elasticsearch/'):
                # BE SURE TO MATCH THE PLUGLIN WITH ES VERSION
                # https://github.com/elasticsearch/elasticsearch-cloud-aws
                sudo('bin/plugin -install elasticsearch/elasticsearch-cloud-aws/2.7.1')

            #REMOVE THESE FILES, WE WILL REPLACE THEM WITH THE CORRECT VERSIONS AT THE END
            sudo("rm -f /usr/local/elasticsearch/config/elasticsearch.yml")
            sudo("rm -f /usr/local/elasticsearch/bin/elasticsearch.in.sh")

        self.conn = self.instance.connection

        # MOUNT AND FORMAT THE EBS VOLUMES (list with `lsblk`)
        for i, k in enumerate(volumes):
            if not fabric_files.exists(k.path):
                sudo('yes | sudo mkfs -t ext4 '+k.device)
                sudo('mkdir '+k.path)
                sudo('sudo mount '+k.device+' '+k.path)

                #ADD TO /etc/fstab SO AROUND AFTER REBOOT
                sudo("sed -i '$ a\\"+k.device+"   "+k.path+"       ext4    defaults,nofail  0   2' /etc/fstab")

        #TEST IT IS WORKING
        sudo('mount -a')

        #INCREASE THE FILE HANDLE LIMITS
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

        sudo("sysctl -p")

        # INCREASE FILE HANDLE PERMISSIONS
        sudo("sed -i '$ a\\ec2-user soft nofile 50000' /etc/security/limits.conf")
        sudo("sed -i '$ a\\ec2-user hard nofile 100000' /etc/security/limits.conf")
        sudo("sed -i '$ a\\ec2-user memlock unlimited' /etc/security/limits.conf")
        sudo("sed -i '$ a\\root soft nofile 50000' /etc/security/limits.conf")
        sudo("sed -i '$ a\\root hard nofile 100000' /etc/security/limits.conf")
        sudo("sed -i '$ a\\root memlock unlimited' /etc/security/limits.conf")

        # EFFECTIVE LOGIN TO LOAD CHANGES TO FILE HANDLES
        # sudo("sudo -i -u ec2-user")

        if not fabric_files.exists("/data1/logs"):
            sudo('mkdir /data1/logs')
            sudo('mkdir /data1/heapdump')

            #INCREASE NUMBER OF FILE HANDLES
            # sudo("sysctl -w fs.file-max=64000")
        # COPY CONFIG FILE TO ES DIR
        if not fabric_files.exists("/usr/local/elasticsearch/config/elasticsearch.yml"):
            yml = File("./examples/config/es_config.yml").read().replace("\r", "")
            yml = expand_template(yml, {
                "id": Random.hex(length=8),
                "data_paths": ",".join("/data"+unicode(i+1) for i, _ in enumerate(volumes))
            })
            File("./results/temp/elasticsearch.yml").write(yml)
            put("./results/temp/elasticsearch.yml", '/usr/local/elasticsearch/config/elasticsearch.yml', use_sudo=True)

        # FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
        # THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
        if not fabric_files.exists("/usr/local/elasticsearch/bin/elasticsearch.in.sh"):
            sh = File("./examples/config/es_run.sh").read().replace("\r", "")
            sh = expand_template(sh, {"memory": unicode(int(gigabytes/2))})
            File("./results/temp/elasticsearch.in.sh").write(sh)
            with cd("/home/ec2-user"):
                put("./results/temp/elasticsearch.in.sh", './temp/elasticsearch.in.sh', use_sudo=True)
                sudo("cp -f ./temp/elasticsearch.in.sh /usr/local/elasticsearch/bin/elasticsearch.in.sh")

    def _install_indexer(self):
        Log.note("Install indexer at {{instance_id}} ({{address}})", instance_id=self.instance.id, address=self.instance.ip_address)
        self._install_python()

        if not fabric_files.exists("/home/ec2-user/ActiveData-ETL/"):
            with cd("/home/ec2-user"):
                sudo("yum -y install git")
                run("git clone https://github.com/klahnakoski/ActiveData-ETL.git")

        with cd("/home/ec2-user/ActiveData-ETL/"):
            run("git checkout push-to-es")
            sudo("pip install -r requirements.txt")

        put("~/private_active_data_etl.json", "/home/ec2-user/private.json")

    def _install_python(self):
        Log.note("Install Python at {{instance_id}} ({{address}})", instance_id=self.instance.id, address=self.instance.ip_address)
        if not fabric_files.exists("/usr/bin/pip"):
            sudo("yum -y install python27")

            run("rm -fr /home/ec2-user/temp")
            run("mkdir  /home/ec2-user/temp")
            with cd("/home/ec2-user/temp"):
                run("wget https://bootstrap.pypa.io/get-pip.py")
                sudo("python27 get-pip.py")

                sudo("ln -s /usr/local/bin/pip /usr/bin/pip")

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
