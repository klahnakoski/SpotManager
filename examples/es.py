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

from fabric.context_managers import cd, hide, shell_env
from fabric.contrib import files as fabric_files
from fabric.operations import sudo, run, put
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
            self._start_es()

            with hide('output'):
                self._install_indexer()
                self._start_indexer()

    def _config_fabric(self, instance):
        if not instance.ip_address:
            Log.error("Expecting an ip address for {{instance_id}}", instance_id=instance.id)

        for k, v in self.settings.connect.items():
            env[k] = v
        env.host_string = instance.ip_address
        env.abort_exception = Log.error

    def _install_es(self, gigabytes):
        volumes = self.instance.markup.drives

        if not fabric_files.exists("/home/ec2-user/temp"):
            with cd("/home/ec2-user/"):
                run("mkdir temp")

            with cd("/home/ec2-user/temp"):
                run('wget -c --no-cookies --no-check-certificate --header "Cookie: s_cc=true; s_nr=1425654197863; s_sq=%5B%5BB%5D%5D; oraclelicense=accept-securebackup-cookie; gpw_e24=http%3A%2F%2Fwww.oracle.com%2Ftechnetwork%2Fjava%2Fjavase%2Fdownloads%2Fjre8-downloads-2133155.html" "http://download.oracle.com/otn-pub/java/jdk/8u40-b25/jre-8u40-linux-x64.rpm" --output-document="jdk-8u5-linux-x64.rpm"')
                sudo("rpm -i jdk-8u5-linux-x64.rpm")
                sudo("alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000")
                run("export JAVA_HOME=/usr/java/default")

            with cd("/home/ec2-user/"):
                run('wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.2.tar.gz')
                run('tar zxfv elasticsearch-1.4.2.tar.gz')
                sudo('mkdir /usr/local/elasticsearch')
                sudo('cp -R elasticsearch-1.4.2/* /usr/local/elasticsearch/')

            with cd('/usr/local/elasticsearch/'):
                # BE SURE TO MATCH THE PLUGLIN WITH ES VERSION
                # https://github.com/elasticsearch/elasticsearch-cloud-aws
                sudo('bin/plugin -install elasticsearch/elasticsearch-cloud-aws/2.4.1')

        if not fabric_files.exists("/data1"):
            self.conn = self.instance.connection

            #MOUNT AND FORMAT THE EBS VOLUME (list with `lsblk`)
            for i, k in enumerate(volumes):

                sudo('mkfs -t ext4 '+k.device)
                sudo('mkdir '+k.path)

                #ADD TO /etc/fstab SO AROUND AFTER REBOOT
                sudo("sed -i '$ a\\"+k.device+"   "+k.path+"       ext4    defaults,nofail  0   2' /etc/fstab")


            #TEST IT IS WORKING
            sudo('mount -a')

            sudo('mkdir /data1/logs')
            sudo('mkdir /data1/heapdump')

            #INCREASE NUMBER OF FILE HANDLES
            sudo("sysctl -w fs.file-max=64000")

        # COPY CONFIG FILE TO ES DIR
        yml = File("./examples/config/es_config.yml").read().replace("\r", "")
        yml = expand_template(yml, {
            "id": Random.hex(length=8),
            "data_paths": ",".join("/data"+unicode(i+1) for i, _ in enumerate(volumes))
        })
        File("./results/temp/elasticsearch.yml").write(yml)
        put("./results/temp/elasticsearch.yml", '/usr/local/elasticsearch/config/elasticsearch.yml', use_sudo=True)

        # FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
        # THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
        sh = File("./examples/config/es_run.sh").read().replace("\r", "")
        sh = expand_template(sh, {"memory": unicode(int(gigabytes))})
        File("./results/temp/elasticsearch.in.sh").write(sh)
        put("./results/temp/elasticsearch.in.sh", '/usr/local/elasticsearch/bin/elasticsearch.in.sh', use_sudo=True)

    def _start_es(self):
        File("./results/temp/start_es.sh").write("nohup ./bin/elasticsearch >& /dev/null < /dev/null &\nsleep 20")
        with cd("/home/ec2-user/"):
            put("./results/temp/start_es.sh", "start_es.sh")
            run("chmod u+x start_es.sh")

        with cd("/usr/local/elasticsearch/"):
            sudo("/home/ec2-user/start_es.sh")

    def _install_indexer(self):
        Log.note("Install indexer at {{instance_id}} ({{address}})", instance_id=self.instance.id, address=self.instance.ip_address)
        if not fabric_files.exists("/usr/bin/pip"):
            sudo("yum -y install python27")

            run("rm -fr /home/ec2-user/temp")
            run("mkdir  /home/ec2-user/temp")
            with cd("/home/ec2-user/temp"):
                run("wget https://bootstrap.pypa.io/get-pip.py")
                sudo("python27 get-pip.py")

                sudo("ln -s /usr/local/bin/pip /usr/bin/pip")

        if not fabric_files.exists("/home/ec2-user/TestLog-ETL/"):
            with cd("/home/ec2-user"):
                sudo("yum -y install git")
                run("git clone https://github.com/klahnakoski/TestLog-ETL.git")

        with cd("/home/ec2-user/TestLog-ETL/"):
            run("git checkout push-to-es")
            sudo("pip install -r requirements.txt")

        put("~/private_active_data_etl.json", "/home/ec2-user/private.json")

    def _start_indexer(self):
        with cd("/home/ec2-user/TestLog-ETL/"):
            run("git pull origin push-to-es")

            with shell_env(PYTHONPATH="."):
                self._run_remote("python27 testlog_etl/push_to_es.py --settings=resources/settings/push_to_es_staging_settings.json", "push_to_es")

    def _run_remote(self, command, name):
        File("./results/temp/"+name+".sh").write("nohup "+command +" >& /dev/null < /dev/null &\nsleep 20")
        put("./results/temp/"+name+".sh", ""+name+".sh")
        run("chmod u+x "+name+".sh")
        run("./"+name+".sh")


