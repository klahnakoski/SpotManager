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
from math import log10

import boto
from boto.ec2.networkinterface import NetworkInterfaceSpecification, NetworkInterfaceCollection
from boto.ec2.spotpricehistory import SpotPriceHistory
from boto.utils import ISO8601
from fabric.api import settings as fabric_settings
from fabric.context_managers import cd
from fabric.contrib import files as fabric_files
from fabric.operations import run, sudo, put
from fabric.state import env

from pyLibrary import aws, convert
from pyLibrary.collections import SUM
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.debugs.startup import SingleInstance
from pyLibrary.dot import wrap, dictwrap, coalesce, listwrap, unwrap, DictList
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.queries.expressions import CODE
from pyLibrary.strings import between
from pyLibrary.thread.threads import Lock, Thread, MAIN_THREAD, Signal
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY, HOUR, WEEK


MIN_UTILITY_PER_DOLLAR = 8 * 7  # 8cpu per dollar (on demand price) multiply by expected 7x savings


class SpotManager(object):
    @use_settings
    def __init__(self, settings):
        self.settings = settings
        self.conn = boto.ec2.connect_to_region(
            region_name=settings.aws.region,
            aws_access_key_id=settings.aws.aws_access_key_id,
            aws_secret_access_key=settings.aws.aws_secret_access_key
        )
        self.price_locker = Lock()
        self.prices = None
        self.done_spot_requests = Signal()
        self._start_life_cycle_watcher()

    def _get_managed_instances(self):
        output =[]
        reservations = self.conn.get_all_instances()
        for res in reservations:
            for instance in res.instances:
                if instance.tags.get('Name', '').startswith(self.settings.ec2.instance.name):
                    output.append(dictwrap(instance))
        return wrap(output)



    def remove_extra_instances(self, spot_requests, utility_to_remove, prices):
        # FIND THE BIGGEST, MOST EXPENSIVE REQUESTS
        instances = self._get_managed_instances()

        for r in instances:
            r.markup = self.price_lookup[r.instance_type]

        instances = qb.sort(instances, [
            {"value": "markup.type.utility", "sort": -1},
            {"value": "markup.estimated_value", "sort": -1}
        ])

        # FIND COMBO THAT WILL SHUTDOWN WHAT WE NEED EXACTLY, OR MORE
        remove_list = []
        for acceptable_error in range(0, 8):
            remaining_utility = utility_to_remove
            remove_list = DictList()
            for s in instances:
                utility = coalesce(s.markup.type.utility, 0)
                if utility <= remaining_utility + acceptable_error:
                    remove_list.append(s)
                    remaining_utility -= utility
            if remaining_utility <= 0:
                break

        # SEND SHUTDOWN TO EACH INSTANCE
        for i in remove_list:
            self.teardown_instance(i)

        remove_requests = remove_list.spot_instance_request_id

        # TERMINATE INSTANCES
        self.conn.terminate_instances(instance_ids=remove_list.id)

        # TERMINATE SPOT REQUESTS
        self.conn.cancel_spot_instance_requests(request_ids=remove_requests)

        return -remaining_utility  # RETURN POSITIVE NUMBER IF TOOK AWAY TOO MUCH


    def update_spot_requests(self, utility_required):
        # how many do we have?
        prices = self.pricing()

        #DO NOT GO OVER BUDGET
        remaining_budget = self.settings.budget

        spot_requests = wrap([dictwrap(r) for r in self.conn.get_all_spot_instance_requests()])
        # instances = wrap([dictwrap(i) for r in self.conn.get_all_instances() for i in r.instances])

        # ADD UP THE CURRENT REQUESTED INSTANCES
        active = qb.filter(spot_requests, {"terms": {"status.code": RUNNING_STATUS_CODES | PENDING_STATUS_CODES}})
        # running = instances.filter(lambda i: i.id in active.instance_id and i._state.name == "running")
        current_spending = coalesce(SUM(self.price_lookup[r.launch_specification.instance_type].current_price for r in active), 0)
        remaining_budget -= current_spending

        current_utility = coalesce(SUM(self.price_lookup[r.launch_specification.instance_type].type.utility for r in active), 0)
        net_new_utility = utility_required - current_utility

        if net_new_utility < 1:  # ONLY REMOVE UTILITY IF WE NEED NONE
            net_new_utility += self.remove_extra_instances(spot_requests, -net_new_utility, prices)

        #what new spot requests are required?
        while net_new_utility > 1:
            for p in prices:

                max_bid = Math.min(p.higher_price, p.type.utility * self.settings.max_utility_price)
                min_bid = p.price_80

                if min_bid > max_bid:
                    Log.note("{{type}} @ {{price|round(decimal=4)}}/hour is over budget of {{limit}}", {
                        "type": p.type.instance_type,
                        "price": min_bid,
                        "limit": p.type.utility * self.settings.max_utility_price
                    })
                    continue

                num = Math.floor(net_new_utility / p.type.utility)
                if num == 1:
                    min_bid = max_bid
                    price_interval = 0
                else:
                    #mid_bid = coalesce(mid_bid, max_bid)
                    price_interval = (max_bid - min_bid) / (num - 1)

                for i in range(num):
                    bid = min_bid + (i * price_interval)
                    if bid < p.current_price or bid > remaining_budget:
                        continue

                    self._request_spot_instance(
                        price=bid,
                        availability_zone_group=p.availability_zone,
                        instance_type=p.type.instance_type,
                        settings=self.settings.ec2.request
                    )
                    net_new_utility -= p.type.utility
                    remaining_budget -= p.current_price

        if net_new_utility > 0:
            Log.warning("Can not fund {{num}} more utility (all utility costs more than {{expected}}/hour)", {
                "num": net_new_utility,
                "expected": 1 / MIN_UTILITY_PER_DOLLAR
            })

        Log.note("All requests for new utility have been made")
        self.done_spot_requests.go()

    def _start_life_cycle_watcher(self):
        def life_cycle_watcher(please_stop):
            self.pricing()

            while not please_stop:
                spot_requests = wrap([dictwrap(r) for r in self.conn.get_all_spot_instance_requests()])
                instances = wrap([dictwrap(i) for r in self.conn.get_all_instances() for i in r.instances])

                #INSTANCES THAT REQUIRE SETUP
                please_setup = instances.filter(lambda i: i.id in spot_requests.instance_id and not i.tags.get("Name") and i._state.name == "running")
                for i in please_setup:
                    try:
                        p = self.price_lookup[i.instance_type]
                        self.setup_instance(i, p.type.utility)
                        i.add_tag("Name", self.settings.ec2.instance.name + " (running)")
                    except Exception, e:
                        Log.warning("problem with setup of {{instance_id}}", {"instance_id": i.id})

                pending = qb.filter(spot_requests, {"terms": {"status.code": PENDING_STATUS_CODES}})
                if not pending and self.done_spot_requests:
                    Log.note("No more pending spot requests")
                    please_stop.go()
                    break
                Thread.sleep(seconds=5, please_stop=please_stop)

            Log.note("life cycle watcher has stopped")

        self.watcher = Thread.run("lifecycle watcher", life_cycle_watcher)


    @use_settings
    def _request_spot_instance(self, price, availability_zone_group, instance_type, settings=None):
        settings.network_interfaces = NetworkInterfaceCollection(
            *unwrap(NetworkInterfaceSpecification(**unwrap(s)) for s in listwrap(settings.network_interfaces))
        )
        settings.settings = None
        return self.conn.request_spot_instances(**unwrap(settings))

    def pricing(self):
        with self.price_locker:
            if self.prices:
                return self.prices

            prices = self._get_spot_prices_from_aws()

            hourly_pricing = qb.run({
                "from": {
                    # AWS PRICING ONLY SENDS timestamp OF CHANGES, MATCH WITH NEXT INSTANCE
                    "from": prices,
                    "window": {
                        "name": "expire",
                        "value": CODE("coalesce(rows[rownum+1].timestamp, Date.eod())"),
                        "edges": ["availability_zone", "instance_type"],
                        "sort": "timestamp"
                    }
                },
                "edges": [
                    "availability_zone",
                    "instance_type",
                    {
                        "name": "time",
                        "range": {"min": "timestamp", "max": "expire", "mode": "inclusive"},
                        "domain": {"type": "time", "min": Date.now().floor(HOUR) - DAY, "max": Date.now().floor(HOUR), "interval": "hour"}
                    }
                ],
                "select": [
                    {"value": "price", "aggregate": "max"},
                    {"aggregate": "count"}
                ],
                "where": {"gt": {"timestamp": Date.now().floor(HOUR) - DAY}},
                "window": {
                    "name": "current_price", "value": CODE("rows.last().price"), "edges": ["availability_zone", "instance_type"], "sort": "time",
                }
            }).data

            bid80 = qb.run({
                "from": hourly_pricing,
                "edges": [
                    {
                        "value": "availability_zone",
                        "allowNulls": False
                    },
                    {
                        "name": "type",
                        "value": "instance_type",
                        "allowNulls": False,
                        "domain": {"type": "set", "key": "instance_type", "partitions": self.settings.utility}
                    }
                ],
                "select": [
                    {"name": "price_80", "value": "price", "aggregate": "percentile", "percentile": 0.80},
                    {"name": "max_price", "value": "price", "aggregate": "max"},
                    {"aggregate": "count"},
                    {"value": "current_price", "aggregate": "one"},
                    {"name": "all_price", "value": "price", "aggregate": "list"}
                ],
                "window": [
                    {"name": "estimated_value", "value": {"div": ["type.utility", "price_80"]}},
                    {"name": "higher_price", "value": lambda row: find_higher(row.all_price, row.price_80)}
                ]
            })

            output = qb.run({
                "from": bid80.data,
                "sort": {"value": "estimated_value", "sort": -1}
            })

            self.prices = output.data
            self.price_lookup = {p.type.instance_type: p for p in self.prices}
            return self.prices

    def _get_spot_prices_from_aws(self):
        try:
            content = File(self.settings.price_file).read()
            cache = convert.json2value(content, flexible=False, paths=False)
        except Exception, e:
            cache = DictList()

        most_recents = qb.run({
            "from": cache,
            "edges": ["instance_type"],
            "select": {"value": "timestamp", "aggregate": "max"}
        }).data


        prices = set(cache)
        for instance_type in self.settings.utility.instance_type:
            if most_recents:
                most_recent = most_recents[{"instance_type":instance_type}].timestamp
                if most_recent == None:
                    start_at = Date.today() - WEEK
                else:
                    start_at = Date(most_recent)
            else:
                start_at = Date.today() - WEEK
            Log.note("get pricing for {{instance_type}} starting at {{start_at}}", {
                "instance_type": instance_type,
                "start_at": start_at
            })

            next_token=None
            while True:
                resultset = self.conn.get_spot_price_history(
                    product_description="Linux/UNIX",
                    instance_type=instance_type,
                    availability_zone="us-west-2c",
                    start_time=start_at.format(ISO8601),
                    next_token=next_token
                )
                next_token = resultset.next_token

                for p in resultset:
                    prices.add(wrap({
                        "availability_zone": p.availability_zone,
                        "instance_type": p.instance_type,
                        "price": p.price,
                        "product_description": p.product_description,
                        "region": p.region.name,
                        "timestamp": Date(p.timestamp)
                    }))

                if not next_token:
                    break


        summary = qb.run({
            "from": prices,
            "edges": ["instance_type"],
            "select": {"value": "instance_type", "aggregate": "count"}
        })
        min_time = Math.MIN(wrap(list(prices)).timestamp)

        File(self.settings.price_file).write(convert.value2json(prices, pretty=True))
        return prices

    def _config_fabric(self, instance):
        for k, v in self.settings.ec2.instance.connect.items():
            env[k] = v
        env.host_string = instance.ip_address
        env.abort_exception = Log.error


    def teardown_instance(self, instance):
        self._config_fabric(instance)
        sudo("supervisorctl stop all")


    def setup_instance(self, instance, utility):
        cpu_count = int(round(utility))

        self._config_fabric(instance)
        self._setup_etl_code()
        self._add_private_file()
        self._setup_etl_supervisor(cpu_count)

    def _setup_etl_code(self):
        sudo("sudo apt-get update")

        if not fabric_files.exists("/home/ubuntu/temp"):
            run("mkdir -p /home/ubuntu/temp")

            with cd("/home/ubuntu/temp"):
                # INSTALL FROM CLEAN DIRECTORY
                run("wget https://bootstrap.pypa.io/get-pip.py")
                sudo("python get-pip.py")

        if not fabric_files.exists("/home/ubuntu/TestLog-ETL"):
            with cd("/home/ubuntu"):
                sudo("apt-get -y install git-core")
                run("git clone https://github.com/klahnakoski/TestLog-ETL.git")

        with cd("/home/ubuntu/TestLog-ETL"):
            run("git checkout etl")
            # pip install -r requirements.txt HAS TROUBLE IMPORTING SOME LIBS
            sudo("pip install MozillaPulse")
            sudo("pip install boto")
            sudo("pip install requests")
            sudo("apt-get -y install python-psycopg2")

    def _setup_etl_supervisor(self, cpu_count):
        # INSTALL supervsor
        sudo("apt-get install -y supervisor")
        with fabric_settings(warn_only=True):
            run("service supervisor start")

        # READ LOCAL CONFIG FILE, ALTER IT FOR THIS MACHINE RESOURCES, AND PUSH TO REMOTE
        conf_file = File("./resources/supervisor/etl.conf")
        content = conf_file.read_bytes()
        find = between(content, "numprocs=", "\n")
        content = content.replace("numprocs=" + find + "\n", "numprocs=" + str(cpu_count * 2) + "\n")
        File("./resources/supervisor/etl.conf.alt").write_bytes(content)
        sudo("rm -f /etc/supervisor/conf.d/etl.conf")
        put("./resources/supervisor/etl.conf.alt", '/etc/supervisor/conf.d/etl.conf', use_sudo=True)
        run("mkdir -p /home/ubuntu/TestLog-ETL/results/logs")

        # POKE supervisor TO NOTICE THE CHANGE
        sudo("supervisorctl reread")
        sudo("supervisorctl update")

    def _add_private_file(self):
        put('~/private.json', '/home/ubuntu')
        with cd("/home/ubuntu"):
            run("chmod o-r private.json")


def find_higher(candidates, reference):
    """
    RETURN ONE PRICE HIGHER THAN reference
    """
    output = wrap([c for c in candidates if c > reference])[0]
    return output


TERMINATED_STATUS_CODES = {
    "capacity-oversubscribed",
    "capacity-not-available",
    "instance-terminated-capacity-oversubscribed",
    "bad-parameters"
}
RETRY_STATUS_CODES = {
    "instance-terminated-by-price",
    "price-too-low",
    "bad-parameters",
    "canceled-before-fulfillment",
    "instance-terminated-by-user"
}
PENDING_STATUS_CODES = {
    "pending-evaluation",
    "pending-fulfillment"
}
RUNNING_STATUS_CODES = {
    "fulfilled"
}


def main():
    """
    CLEAR OUT KEYS FROM BUCKET BY RANGE, OR BY FILE
    """
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        with SingleInstance():
            m = SpotManager(settings)

            queue = aws.Queue(settings.work_queue)
            pending = len(queue)
            # SINCE EACH ITEM IN QUEUE REPRESENTS SMALL, OR GIGANTIC, AMOUNT
            # OF TOTAL WORK THE QUEUE SIZE IS TERRIBLE PREDICTOR OF HOW MUCH
            # UTILITY WE REALLY NEED.  WE USE log10() TO SUPPRESS THE
            # VARIABILITY, AND HOPE FOR THE BEST
            utility_required = max(2, log10(max(pending, 1)) * 10)

            m.update_spot_requests(utility_required)
    except Exception, e:
        Log.warning("Problem with spot manager", e)
    finally:
        Log.stop()
        MAIN_THREAD.stop()


if __name__ == "__main__":
    main()
