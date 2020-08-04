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
from mo_files import File, TempFile
from mo_future import text
from mo_kwargs import override
from mo_logs import Log
from mo_logs.strings import expand_template
import mo_math
from spot.instance_manager import InstanceManager

JRE = "jre-8u131-linux-x64.rpm"
PYPY_DIR = "pypy2.7-v7.3.0-linux64"
PYPY_BZ2 = "pypy2.7-v7.3.0-linux64.tar.bz2"
RESOURCES = File("resources")


class ES6Spot(InstanceManager):
    """
    THIS CLASS MUST HAVE AN IMPLEMENTATION FOR the SpotManager TO USE
    """
    @override
    def __init__(self, minimum_utility, kwargs=None):
        InstanceManager.__init__(self, kwargs)
        self.settings = kwargs
        self.minimum_utility = minimum_utility

    def required_utility(self, current_utility=None):
        return self.minimum_utility

    def setup(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO SETUP
        utility,    # THE utility OBJECT FOUND IN CONFIG
        please_stop
    ):
        try:
            with Connection(host=instance.ip_address, kwargs=self.settings.connect) as conn:
                gigabytes = mo_math.floor(utility.memory)
                Log.note("setup {{instance}}", instance=instance.id)

                _install_python_indexer(instance=instance, conn=conn)
                _install_es(gigabytes, instance=instance, conn=conn)
                _install_supervisor(instance=instance, conn=conn)
                _start_supervisor(conn=conn)
                Log.alert("Done install of {{host}}", host=instance.ip_address)
        except Exception as e:
            Log.error("could not setup ES at {{ip}}", ip=instance.ip_address, cause=e)

    def teardown(
        self,
        instance,   # THE boto INSTANCE OBJECT FOR THE MACHINE TO TEARDOWN
        please_stop
    ):
        with Connection(host=instance.ip_address, kwargs=self.settings.connect) as conn:
            Log.note("teardown {{instance}}", instance=instance.id)

            # ASK NICELY TO STOP Elasticsearch PROCESS
            conn.sudo("supervisorctl stop push-to-es:*", warn=True)
            # ASK NICELY TO STOP Elasticsearch PROCESS
            conn.sudo("supervisorctl stop es:*", warn=True)

            # ASK NICELY TO STOP "supervisord" PROCESS
            pid = conn.sudo("ps -ef | grep supervisord | grep -v grep | awk '{print $2}'", warn=True).stdout.strip()
            Log.note("shutdown supervisor at pid={{pid}}", pid=pid)
            conn.sudo("kill -SIGINT " + pid, warn=True)

            # WAIT FOR SUPERVISOR SHUTDOWN
            pid = True
            while pid:
                pid = conn.sudo("ps -ef | grep supervisord | grep -v grep | awk '{print $2}'").stdout.strip()

def _install_es(gigabytes, es_version="6.5.4", instance=None, conn=None):
    es_file = 'elasticsearch-' + es_version + '.tar.gz'
    volumes = instance.markup.drives

    if not conn.exists("/usr/local/elasticsearch/config/elasticsearch.yml"):
        with conn.cd("/home/ec2-user/"):
            conn.run("mkdir -p temp")

        if not (RESOURCES / JRE).exists:
            Log.error("Expecting {{file}} on manager to spread to ES instances", file=(RESOURCES / JRE))
        response = conn.run("java -version", warn=True)
        if "Java(TM) SE Runtime Environment" not in response:
            with conn.cd("/home/ec2-user/temp"):
                conn.run('rm -f '+JRE)
                conn.put((RESOURCES / JRE), JRE)
                conn.sudo("rpm -i "+JRE)
                conn.sudo("alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000")
                conn.run("export JAVA_HOME=/usr/java/default")

        with conn.cd("/home/ec2-user/"):
            conn.put(RESOURCES / es_file, es_file)
            conn.run('tar zxfv ' + es_file)
            conn.sudo("rm -fr /usr/local/elasticsearch", warn=True)
            conn.sudo('mkdir /usr/local/elasticsearch')
            conn.sudo('cp -R elasticsearch-'+es_version+'/* /usr/local/elasticsearch/')

        with conn.cd('/usr/local/elasticsearch/'):
            # BE SURE TO MATCH THE PLUGLIN WITH ES VERSION
            # https://github.com/elasticsearch/elasticsearch-cloud-aws
            conn.sudo('sudo bin/elasticsearch-plugin install -b discovery-ec2')

        # REMOVE THESE FILES, WE WILL REPLACE THEM WITH THE CORRECT VERSIONS AT THE END
        conn.sudo("rm -f /usr/local/elasticsearch/config/elasticsearch.yml")
        conn.sudo("rm -f /usr/local/elasticsearch/config/jvm.options")
        conn.sudo("rm -f /usr/local/elasticsearch/config/log4j2.properties")

    # MOUNT AND FORMAT THE VOLUMES (list with `lsblk`)
    for i, k in enumerate(volumes):
        if not conn.exists(k.path):
            # ENSURE DEVICE IS NOT MOUNTED
            conn.sudo('sudo umount '+k.device, warn=True)

            # (RE)PARTITION THE LOCAL DEVICE, AND FORMAT
            conn.sudo("parted " + k.device + " --script \"mklabel gpt mkpart primary ext4 2048s 100%\"")
            conn.sudo('yes | sudo mkfs -t ext4 '+k.device)

            # ES AND JOURNALLING DO NOT MIX
            conn.sudo('tune2fs -o journal_data_writeback '+k.device)
            conn.sudo('tune2fs -O ^has_journal '+k.device)

            # MOUNT IT
            conn.sudo('mkdir '+k.path)
            conn.sudo('sudo mount '+k.device+' '+k.path)
            conn.sudo('chown -R ec2-user:ec2-user '+k.path)

            # ADD TO /etc/fstab SO AROUND AFTER REBOOT
            conn.sudo("sed -i '$ a\\"+k.device+"   "+k.path+"       ext4    defaults,nofail  0   2' /etc/fstab")

    # TEST IT IS WORKING
    conn.sudo('mount -a')

    # INCREASE THE FILE HANDLE LIMITS
    with conn.cd("/home/ec2-user/"):
        with TempFile() as temp:
            conn.get("/etc/sysctl.conf", temp, use_sudo=True)
            lines = temp.read()
            if lines.find("fs.file-max = 100000") == -1:
                lines += "\nfs.file-max = 100000"
            lines = lines.replace("net.bridge.bridge-nf-call-ip6tables = 0", "")
            lines = lines.replace("net.bridge.bridge-nf-call-iptables = 0", "")
            lines = lines.replace("net.bridge.bridge-nf-call-arptables = 0", "")
            temp.write(lines)
            conn.put(temp, "/etc/sysctl.conf", use_sudo=True)

    conn.sudo("sudo sed -i '$ a\\vm.max_map_count = 262144' /etc/sysctl.conf")

    conn.sudo("sysctl -p")

    # INCREASE FILE HANDLE PERMISSIONS
    conn.sudo("sed -i '$ a\\root soft nofile 100000' /etc/security/limits.conf")
    conn.sudo("sed -i '$ a\\root hard nofile 100000' /etc/security/limits.conf")
    conn.sudo("sed -i '$ a\\root soft memlock unlimited' /etc/security/limits.conf")
    conn.sudo("sed -i '$ a\\root hard memlock unlimited' /etc/security/limits.conf")

    conn.sudo("sed -i '$ a\\ec2-user soft nofile 100000' /etc/security/limits.conf")
    conn.sudo("sed -i '$ a\\ec2-user hard nofile 100000' /etc/security/limits.conf")
    conn.sudo("sed -i '$ a\\ec2-user soft memlock unlimited' /etc/security/limits.conf")
    conn.sudo("sed -i '$ a\\ec2-user hard memlock unlimited' /etc/security/limits.conf")

    if not conn.exists("/data1/logs"):
        conn.run('mkdir /data1/logs')
        conn.run('mkdir /data1/heapdump')

    # COPY CONFIG FILES TO ES DIR
    if not conn.exists("/usr/local/elasticsearch/config/elasticsearch.yml"):
        conn.put("./examples/config/es6_log4j2.properties", '/usr/local/elasticsearch/config/log4j2.properties', use_sudo=True)

        jvm = File("./examples/config/es6_jvm.options").read().replace('\r', '')
        jvm = expand_template(jvm, {"memory": int(gigabytes/2)})
        with TempFile() as temp:
            temp.write(jvm)
            conn.put(temp, '/usr/local/elasticsearch/config/jvm.options', use_sudo=True)

        yml = File("./examples/config/es6_config.yml").read().replace("\r", "")
        yml = expand_template(yml, {
            "id": instance.ip_address,
            "data_paths": ",".join("/data" + text(i + 1) for i, _ in enumerate(volumes))
        })
        with TempFile() as temp:
            temp.write(yml)
            conn.put(temp, '/usr/local/elasticsearch/config/elasticsearch.yml', use_sudo=True)

    conn.sudo("chown -R ec2-user:ec2-user /usr/local/elasticsearch")

def _install_python_indexer(instance, conn):
    Log.note("Install Python at {{instance_id}} ({{address}})", instance_id=instance.id, address=instance.ip_address)
    _install_python(instance, conn)

    if not conn.exists("/home/ec2-user/ActiveData-ETL/"):
        with conn.cd("/home/ec2-user"):
            conn.sudo("yum -y install git")
            conn.run("git clone https://github.com/klahnakoski/ActiveData-ETL.git")

    with conn.cd("/home/ec2-user/ActiveData-ETL/"):
        conn.run("git checkout push-to-es6")
        conn.sudo("yum -y install gcc")  # REQUIRED FOR psutil
        conn.run("python -m pip install -r requirements.txt")

    conn.put("~/private_active_data_etl.json", "/home/ec2-user/private.json")

def _install_python(instance, conn):
    Log.note("Install Python at {{instance_id}} ({{address}})", instance_id=instance.id, address=instance.ip_address)
    if conn.exists("/usr/bin/pip"):
        pip_version = text(conn.sudo("pip --version", warn=True))
    else:
        pip_version = ""

    if not pip_version.startswith("pip 20."):
        conn.sudo("yum -y install python2")
        conn.sudo("easy_install pip")
        # conn.sudo("rm -f /usr/bin/pip", warn=True)
        # conn.sudo("ln -s /usr/local/bin/pip /usr/bin/pip")
        # conn.sudo("pip install --upgrade pip")

def _install_pypy(instance, conn):
    Log.note("Install pypy at {{instance_id}} ({{address}})", instance_id=instance.id, address=instance.ip_address)

    if not (RESOURCES / PYPY_BZ2).exists:
        Log.error("Expecting {{file}} on manager to spread to ES instances", file=(RESOURCES / PYPY_BZ2))

    if conn.exists("~/pypy/bin/pip"):
        return

    with conn.cd("/home/ec2-user/"):
        conn.put((RESOURCES / PYPY_BZ2), PYPY_BZ2)
        conn.run('tar jxf ' + PYPY_BZ2)
        conn.run("mv " + PYPY_DIR + " pypy")

    conn.run("rm -fr /home/ec2-user/temp", warn=True)
    conn.run("mkdir /home/ec2-user/temp")
    with conn.cd("/home/ec2-user/temp"):
        conn.run("wget https://bootstrap.pypa.io/get-pip.py")
        conn.run("~/pypy/bin/pypy get-pip.py")

def _install_pypy_indexer(instance, conn):
    Log.note("Install indexer at {{instance_id}} ({{address}})", instance_id=instance.id, address=instance.ip_address)
    _install_pypy(instance, conn)

    if not conn.exists("/home/ec2-user/ActiveData-ETL/"):
        with conn.cd("/home/ec2-user"):
            conn.sudo("yum -y install git")
            conn.run("git clone https://github.com/klahnakoski/ActiveData-ETL.git")

    with conn.cd("/home/ec2-user/ActiveData-ETL/"):
        conn.run("git checkout push-to-es6")
        conn.run("git pull origin push-to-es6")
        conn.sudo("yum -y install gcc")  # REQUIRED FOR psutil
        conn.run("~/pypy/bin/pip install -r requirements.txt")

    conn.put("~/private_active_data_etl.json", "/home/ec2-user/private.json")

def _install_supervisor(instance, conn):
    _install_python(instance, conn)
    conn.sudo("pip install supervisor")

def _install_lib(lib_name, install="install", conn=None):
    """
    :param lib_name:
    :param install: use 'groupinstall' if you wish
    :return:
    """
    result = conn.sudo("yum "+install+" -y "+lib_name, warn=True)
    if result.return_code != 0 and result.find("already installed and latest version") == -1:
        Log.error("problem with install of {{lib}}", lib=lib_name)

def _start_supervisor(conn):
    conn.put("./examples/config/es6_supervisor.conf", "/etc/supervisord.conf", use_sudo=True)

    # START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
    conn.sudo("supervisord -c /etc/supervisord.conf", warn=True)
    conn.sudo("supervisorctl reread")
    conn.sudo("supervisorctl update")

