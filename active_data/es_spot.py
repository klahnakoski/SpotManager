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

import boto

from fabric.api import settings as fabric_settings
from fabric.context_managers import cd, hide
from fabric.contrib import files as fabric_files
from fabric.operations import sudo, run, put
from fabric.state import env

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import unwrap, dictwrap, wrap
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.maths.randoms import Random
from pyLibrary.meta import use_settings
from pyLibrary.strings import expand_template, between
from pyLibrary.thread.threads import Thread, Lock

from spot.instance_manager import InstanceManager


class ESSpot(InstanceManager):
    """
    THIS CLASS MUST HAVE AN IMPLEMENTATION FOR the SpotManager TO USE
    """
    @use_settings
    def __init__(self, settings):
        self.settings = settings
        self.volumes = []
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
            Log.note("setup {{instance}}", {"instance": instance.id})
            with hide('output'):
                self._config_fabric(instance)
                self._install_es(gigabytes)
            self._start_es()

    def _config_fabric(self, instance):
        if not instance.ip_address:
            Log.error("Expecting an ip address for {{instance_id}}", {"instance_id": instance.id})

        for k, v in self.settings.connect.items():
            env[k] = v
        env.host_string = instance.ip_address
        env.abort_exception = Log.error

    def _install_es(self, gigabytes):
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
            self._add_volumes(self.instance, Math.floor(gigabytes/15))

            #MOUNT AND FORMAT THE EBS VOLUME (list with `lsblk`)
            for i, k in enumerate(self.volumes):
                si = unicode(i+1)
                sudo('mkfs -t ext4 /dev/xvd'+k["letter"])
                sudo('mkdir /data'+si)

                #ADD TO /etc/fstab SO AROUND AFTER REBOOT
                sudo("sed -i '$ a\\/dev/xvd"+k["letter"]+"   /data"+si+"       ext4    defaults,nofail  0   2' /etc/fstab")


            #TEST IT IS WORKING
            sudo('mount -a')

            sudo('mkdir /data1/logs')
            sudo('mkdir /data1/heapdump')

            #INCREASE NUMBER OF FILE HANDLES
            sudo("sysctl -w fs.file-max=64000")

        # COPY CONFIG FILE TO ES DIR
        yml = File("./resources/config/es_spot_config.yml").read().replace("\r", "")
        yml = expand_template(yml, {
            "id": Random.hex(length=8),
            "data_paths": ",".join("/data"+unicode(i+1) for i, _ in enumerate(self.volumes))
        })
        File("./results/temp/elasticsearch.yml").write(yml)
        put("./results/temp/elasticsearch.yml", '/usr/local/elasticsearch/config/elasticsearch.yml', use_sudo=True)

        # FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
        # THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
        sh = File("./resources/config/es_spot_run.sh").read().replace("\r", "")
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

    def _add_volumes(self, instance, num_volumes):
        if instance.markup.drives:
            self.volumes = [{"letter": v} for v in instance.markup.drives]
        else:
            volumes = []
            for i in range(num_volumes):
                letter = convert.ascii2char(98 + i)  # START AT 'b'
                v = self.conn.create_volume(**unwrap(self.settings.new_volume))
                volumes.append(wrap({"volume": v, "letter": letter}))

            try:
                status = list(self.conn.get_all_volumes(volume_ids=[v.volume.id for v in volumes]))
                while any(s for s in status if s.status != "available"):
                    Thread.sleep(seconds=5)
                    status = list(self.conn.get_all_volumes(volume_ids=[v.volume.id for v in volumes]))

                for v in volumes:
                    self.conn.attach_volume(v.volume.id, instance.id, "/dev/xvd" + v.letter)
            except Exception, e:
                for v in volumes:
                    self.conn.delete_volume(v.volume.id)
                Log.error("Can not setup", e)

            self.volumes=volumes
