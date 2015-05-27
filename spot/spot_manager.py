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
import boto.vpc
import boto.ec2
from boto.ec2.blockdevicemapping import BlockDeviceType, BlockDeviceMapping
from boto.ec2.networkinterface import NetworkInterfaceSpecification, NetworkInterfaceCollection
from boto.ec2.spotpricehistory import SpotPriceHistory
from boto.utils import ISO8601

from pyLibrary import convert
from pyLibrary.collections import SUM
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.debugs.startup import SingleInstance
from pyLibrary.dot import unwrap, coalesce, DictList, wrap, listwrap, Dict
from pyLibrary.dot.objects import dictwrap
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings, new_instance
from pyLibrary.queries import qb
from pyLibrary.queries.expressions import CODE
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.thread.threads import Lock, Thread, MAIN_THREAD, Signal
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY, HOUR, WEEK, MINUTE, SECOND, Duration
from pyLibrary.times.timer import Timer


DEBUG_PRICING = False
TIME_FROM_RUNNING_TO_LOGIN = 5 * MINUTE
ERROR_ON_CALL_TO_SETUP="Problem with setup()"

class SpotManager(object):
    @use_settings
    def __init__(self, instance_manager, disable_prices=False, settings=None):
        self.settings = settings
        self.instance_manager = instance_manager
        aws_args = dict(
            region_name=settings.aws.region,
            aws_access_key_id=unwrap(settings.aws.aws_access_key_id),
            aws_secret_access_key=unwrap(settings.aws.aws_secret_access_key))
        self.ec2_conn = boto.ec2.connect_to_region(**aws_args)
        self.vpc_conn = boto.vpc.connect_to_region(**aws_args)
        self.price_locker = Lock()
        self.prices = None
        self.price_lookup = None
        self.done_spot_requests = Signal()
        self.net_new_locker = Lock()
        self.net_new_spot_requests = UniqueIndex(("id",))  # SPOT REQUESTS FOR THIS SESSION
        self.watcher = None
        if instance_manager and instance_manager.setup_required():
            self._start_life_cycle_watcher()
        if not disable_prices:
            self.pricing()

    def update_spot_requests(self, utility_required):
        spot_requests = self._get_managed_spot_requests()

        # ADD UP THE CURRENT REQUESTED INSTANCES
        all_instances = UniqueIndex("id", data=self._get_managed_instances())
        active = wrap([r for r in spot_requests if r.status.code in RUNNING_STATUS_CODES | PENDING_STATUS_CODES | PROBABLY_NOT_FOR_A_WHILE])

        for a in active.copy():
            if a.status.code == "request-canceled-and-instance-running" and all_instances[a.instance_id] == None:
                active.remove(a)

        used_budget = 0
        current_spending = 0
        for a in active:
            about = self.price_lookup[a.launch_specification.instance_type, a.launch_specification.placement]
            discount = coalesce(about.type.discount, 0)
            Log.note(
                "Active Spot Request {{id}}: {{type}} {{instance_id}} in {{zone}} @ {{price|round(decimal=4)}}",
                id=a.id,
                type=a.launch_specification.instance_type,
                zone=a.launch_specification.placement,
                instance_id=a.instance_id,
                price=a.price - discount
            )
            used_budget += a.price - discount
            current_spending += coalesce(about.current_price, a.price) - discount

        Log.note(
            "Total Exposure: ${{budget|round(decimal=4)}}/hour (current price: ${{current|round(decimal=4)}}/hour)",
            budget=used_budget,
            current=current_spending
        )

        remaining_budget = self.settings.budget - used_budget

        current_utility = coalesce(SUM(self.price_lookup[r.launch_specification.instance_type, r.launch_specification.placement].type.utility for r in active), 0)
        net_new_utility = utility_required - current_utility

        Log.note("have {{current_utility}} utility running; need {{need_utility}} more utility", current_utility=current_utility, need_utility=net_new_utility)

        if remaining_budget < 0:
            remaining_budget, net_new_utility = self.save_money(remaining_budget, net_new_utility)

        if net_new_utility <= 0:
            net_new_utility = self.remove_instances(net_new_utility)

        if net_new_utility > 0:
            net_new_utility = Math.min(net_new_utility, self.settings.max_new_utility)
            net_new_utility, remaining_budget = self.add_instances(net_new_utility, remaining_budget)

        if net_new_utility > 0:
            Log.alert(
                "Can not fund {{num|round(places=2)}} more utility (all utility costs more than ${{expected|round(decimal=2)}}/hour).  Remaining budget is ${{budget|round(decimal=2)}} ",
                num=net_new_utility,
                expected=self.settings.max_utility_price,
                budget=remaining_budget
            )

        # Give EC2 a chance to notice the new requests before tagging them.
        Thread.sleep(3)
        with self.net_new_locker:
            for req in self.net_new_spot_requests:
                req.add_tag("Name", self.settings.ec2.instance.name)

        Log.note("All requests for new utility have been made")
        self.done_spot_requests.go()

    def add_instances(self, net_new_utility, remaining_budget):
        prices = self.pricing()

        for p in prices:
            if net_new_utility <= 0 or remaining_budget <= 0:
                break

            if p.current_price == None:
                Log.note("{{type}} has no price",
                    type=p.type.instance_type
                )
                continue

            if self.settings.utility[p.type.instance_type].blacklist or \
                    p.availability_zone in listwrap(self.settings.utility[p.type.instance_type].blacklist_zones):
                Log.note("{{type}} in {{zone}} skipped due to blacklist", type=p.type.instance_type, zone=p.availability_zone)
                continue

            # DO NOT BID HIGHER THAN WHAT WE ARE WILLING TO PAY
            max_acceptable_price = p.type.utility * self.settings.max_utility_price
            max_bid = Math.min(p.higher_price, max_acceptable_price)
            min_bid = p.price_80

            if min_bid > max_bid:
                Log.note(
                    "{{type}} @ {{price|round(decimal=4)}}/hour is over budget of {{limit}}",
                    type=p.type.instance_type,
                    price=min_bid,
                    limit=p.type.utility * self.settings.max_utility_price
                )
                continue

            num = Math.min(int(Math.round(net_new_utility / p.type.utility)), coalesce(self.settings.max_requests_per_type, 10000000))
            if num == 1:
                min_bid = Math.min(Math.max(p.current_price * 1.1, min_bid), max_acceptable_price)
                price_interval = 0
            else:
                price_interval = Math.min(min_bid / 10, (max_bid - min_bid) / (num - 1))

            for i in range(num):
                bid = min_bid + (i * price_interval)
                if bid < p.current_price or bid > remaining_budget:
                    continue

                try:
                    new_requests = self._request_spot_instances(
                        price=bid,
                        availability_zone_group=p.availability_zone,
                        instance_type=p.type.instance_type,
                        settings=self.settings.ec2.request
                    )
                    Log.note(
                        "Request {{num}} instance {{type}} in {{zone}} with utility {{utility}} at ${{price}}/hour",
                        num=len(new_requests),
                        type=p.type.instance_type,
                        zone=p.availability_zone,
                        utility=p.type.utility,
                        price=bid
                    )
                    net_new_utility -= p.type.utility * len(new_requests)
                    remaining_budget -= bid * len(new_requests)
                    with self.net_new_locker:
                        for ii in new_requests:
                            self.net_new_spot_requests.add(ii)
                except Exception, e:
                    Log.warning(
                        "Request instance {{type}} failed because {{reason}}",
                        type=p.type.instance_type,
                        reason=e.message,
                        cause=e
                    )

        return net_new_utility, remaining_budget

    def remove_instances(self, net_new_utility):
        instances = self.running_instances()

        # FIND COMBO THAT WILL SHUTDOWN WHAT WE NEED EXACTLY, OR MORE
        remove_list = []
        for acceptable_error in range(0, 8):
            remaining_utility = -net_new_utility
            remove_list = DictList()
            for s in instances:
                utility = coalesce(s.markup.type.utility, 0)
                if utility <= remaining_utility + acceptable_error:
                    remove_list.append(s)
                    remaining_utility -= utility
            if remaining_utility <= 0:
                net_new_utility = -remaining_utility
                break

        if not remove_list:
            return net_new_utility

        # SEND SHUTDOWN TO EACH INSTANCE
        Log.note("Shutdown {{instances}}", instances=remove_list.id)
        for i in remove_list:
            try:
                self.instance_manager.teardown(i)
            except Exception, e:
                Log.warning("Teardown of {{id}} failed", id=i.id, cause=e)

        remove_spot_requests = remove_list.spot_instance_request_id

        # TERMINATE INSTANCES
        self.ec2_conn.terminate_instances(instance_ids=remove_list.id)

        # TERMINATE SPOT REQUESTS
        self.ec2_conn.cancel_spot_instance_requests(request_ids=remove_spot_requests)

        return net_new_utility

    def running_instances(self):
        # FIND THE BIGGEST, MOST EXPENSIVE REQUESTS
        instances = self._get_managed_instances()
        for r in instances:
            try:
                r.markup = self.price_lookup[r.instance_type, r.placement]
            except Exception, e:
                r.markup = self.price_lookup[r.instance_type, r.placement]
                Log.error("No pricing!!!", e)
        instances = qb.sort(instances, [
            {"value": "markup.type.utility", "sort": -1},
            {"value": "markup.estimated_value", "sort": 1}
        ])
        return instances

    def save_money(self, remaining_budget, net_new_utility):
        remove_spot_requests = wrap([])

        # FIRST CANCEL THE PENDING REQUESTS
        if remaining_budget < 0:
            requests = self._get_managed_spot_requests()
            for r in requests:
                if r.status.code in PENDING_STATUS_CODES | PROBABLY_NOT_FOR_A_WHILE:
                    remove_spot_requests.append(r.id)
                    net_new_utility += self.settings.utility[r.launch_specification.instance_type].utility
                    remaining_budget += r.price

        instances = qb.sort(self.running_instances(), "markup.estimated_value")

        remove_list = wrap([])
        for s in instances:
            if remaining_budget >= 0:
                break
            remove_list.append(s)
            net_new_utility += coalesce(s.markup.type.utility, 0)
            remaining_budget += coalesce(s.markup.price_80, s.markup.current_price)

        # SEND SHUTDOWN TO EACH INSTANCE
        Log.note("Shutdown {{instances}}", instances=remove_list.id)
        for i in remove_list:
            try:
                self.instance_manager.teardown(i)
            except Exception, e:
                Log.warning("Teardown of {{id}} failed", id=i.id, cause=e)

        remove_spot_requests.extend(remove_list.spot_instance_request_id)

        # TERMINATE INSTANCES
        self.ec2_conn.terminate_instances(instance_ids=remove_list.id)

        # TERMINATE SPOT REQUESTS
        self.ec2_conn.cancel_spot_instance_requests(request_ids=remove_spot_requests)
        return remaining_budget, net_new_utility

    def _get_managed_spot_requests(self):
        output = wrap([dictwrap(r) for r in self.ec2_conn.get_all_spot_instance_requests() if not r.tags.get("Name") or r.tags.get("Name").startswith(self.settings.ec2.instance.name)])
        return output

    def _get_managed_instances(self):
        output = []
        reservations = self.ec2_conn.get_all_instances()
        for res in reservations:
            for instance in res.instances:
                if instance.tags.get('Name', '').startswith(self.settings.ec2.instance.name) and instance._state.name == "running":
                    output.append(dictwrap(instance))
        return wrap(output)

    def _start_life_cycle_watcher(self):
        def life_cycle_watcher(please_stop):
            failed_attempts=Dict()

            while not please_stop:
                spot_requests = self._get_managed_spot_requests()
                last_get = Date.now()
                instances = wrap({i.id: i for r in self.ec2_conn.get_all_instances() for i in r.instances})
                # INSTANCES THAT REQUIRE SETUP
                time_to_stop_trying = {}
                please_setup = [(i, r) for i, r in [(instances[r.instance_id], r) for r in spot_requests] if i.id and not i.tags.get("Name") and i._state.name == "running"]
                for i, r in please_setup:
                    try:
                        p = self.settings.utility[i.instance_type]
                        i.markup = p
                        try:
                            self.instance_manager.setup(i, p.utility)
                        except Exception, e:
                            failed_attempts[r.id] += [Except.wrap(e)]
                            Log.error(ERROR_ON_CALL_TO_SETUP, e)
                        i.add_tag("Name", self.settings.ec2.instance.name + " (running)")
                        with self.net_new_locker:
                            self.net_new_spot_requests.remove(r.id)
                    except Exception, e:
                        if not time_to_stop_trying.get(i.id):
                            time_to_stop_trying[i.id] = Date.now() + TIME_FROM_RUNNING_TO_LOGIN
                        if Date.now() > time_to_stop_trying[i.id]:
                            # FAIL TO SETUP AFTER x MINUTES, THEN TERMINATE INSTANCE
                            self.ec2_conn.terminate_instances(instance_ids=[i.id])
                            with self.net_new_locker:
                                self.net_new_spot_requests.remove(r.id)
                            Log.warning("Problem with setup of {{instance_id}}.  Time is up.  Instance TERMINATED!", instance_id=i.id, cause=e)
                        elif ERROR_ON_CALL_TO_SETUP in e:
                            if len(failed_attempts[r.id]) > 2:
                                Log.warning("Problem with setup() of {{instance_id}}", instance_id=i.id, cause=failed_attempts[i.id])
                        else:
                            Log.warning("Unexpected failure on startup", instance_id=i.id, cause=e)

                if Date.now() - last_get > 5 * SECOND:
                    # REFRESH STALE
                    spot_requests = self._get_managed_spot_requests()
                    last_get = Date.now()

                pending = wrap([r for r in spot_requests if r.status.code in PENDING_STATUS_CODES])
                give_up = wrap([r for r in spot_requests if r.status.code in PROBABLY_NOT_FOR_A_WHILE])

                if self.done_spot_requests:
                    with self.net_new_locker:
                        expired = Date.now() - self.settings.run_interval + 2 * MINUTE
                        for ii in list(self.net_new_spot_requests):
                            if Date(ii.create_time) < expired:
                                ## SOMETIMES REQUESTS NEVER GET INTO THE MAIN LIST OF REQUESTS
                                self.net_new_spot_requests.remove(ii)
                        for g in give_up:
                            self.net_new_spot_requests.remove(g.id)
                        pending = UniqueIndex(("id",), data=pending)
                        pending = pending | self.net_new_spot_requests

                if give_up:
                    self.ec2_conn.cancel_spot_instance_requests(request_ids=give_up.id)
                    Log.note("Cancelled spot requests {{spots}}", spots=give_up.id)

                if not pending and not time_to_stop_trying and self.done_spot_requests:
                    Log.note("No more pending spot requests")
                    please_stop.go()
                    break
                elif pending:
                    Log.note("waiting for spot requests: {{pending}}", pending=[p.id for p in pending])

                Thread.sleep(seconds=10, please_stop=please_stop)

            Log.note("life cycle watcher has stopped")

        self.watcher = Thread.run("lifecycle watcher", life_cycle_watcher)

    def _get_valid_availability_zones(self):
        subnets = list(self.vpc_conn.get_all_subnets(subnet_ids=self.settings.ec2.request.network_interfaces.subnet_id))
        zones_with_interfaces = [s.availability_zone for s in subnets]

        if self.settings.availability_zone:
            # If they pass a list of zones, constrain it by zones we have an
            # interface for.
            return set(zones_with_interfaces) & set(listwrap(self.settings.availability_zone))
        else:
            # Otherwise, use all available zones.
            return zones_with_interfaces

    @use_settings
    def _request_spot_instances(self, price, availability_zone_group, instance_type, settings):
        settings.network_interfaces = NetworkInterfaceCollection(*(
            NetworkInterfaceSpecification(**unwrap(i))
            for i in listwrap(settings.network_interfaces)
            if self.vpc_conn.get_all_subnets(subnet_ids=i.subnet_id, filters={"availabilityZone": availability_zone_group})
        ))

        if len(settings.network_interfaces) == 0:
            Log.error("No network interface specifications found for {{availability_zone}}!", availability_zone=settings.availability_zone_group)

        settings.settings = None
        settings.block_device_map = BlockDeviceMapping()

        # GENERIC BLOCK DEVICE MAPPING
        for dev, dev_settings in settings.block_device_map.items():
            settings.block_device_map[dev] = BlockDeviceType(**unwrap(dev_settings))

        # INCLUDE EPHEMERAL STORAGE IN BlockDeviceMapping
        num_ephemeral_volumes = ephemeral_storage[instance_type]["num"]
        for i in range(num_ephemeral_volumes):
            letter = convert.ascii2char(98 + i)
            settings.block_device_map["/dev/sd" + letter] = BlockDeviceType(
                ephemeral_name='ephemeral' + unicode(i),
                delete_on_termination=True
            )

        if settings.expiration:
            settings.valid_until = (Date.now() + Duration(settings.expiration)).format(ISO8601)
            settings.expiration = None

        #ATTACH NEW EBS VOLUMES
        for i, drive in enumerate(self.settings.utility[instance_type].drives):
            d = drive.copy()
            d.path = None  # path AND device PROPERTY IS NOT ALLOWED IN THE BlockDeviceType
            d.device = None
            if d.size:
                settings.block_device_map[drive.device] = BlockDeviceType(
                    delete_on_termination=True,
                    **unwrap(d)
                )
        output = list(self.ec2_conn.request_spot_instances(**unwrap(settings)))
        return output

    def pricing(self):
        with self.price_locker:
            if self.prices:
                return self.prices

            prices = self._get_spot_prices_from_aws()

            with Timer("processing pricing data"):
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
                            "allowNulls": False,
                            "domain": {"type": "time", "min": Date.now().floor(HOUR) - DAY, "max": Date.now().floor(HOUR), "interval": "hour"}
                        }
                    ],
                    "select": [
                        {"value": "price", "aggregate": "max"},
                        {"aggregate": "count"}
                    ],
                    "where": {"gt": {"expire": Date.now().floor(HOUR) - DAY}},
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
                        {"name": "price_80", "value": "price", "aggregate": "percentile", "percentile": self.settings.bid_percentile},
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
                self.price_lookup = UniqueIndex(("type.instance_type", "availability_zone"), data=self.prices)
            return self.prices

    def _get_spot_prices_from_aws(self):
        with Timer("Read pricing file"):
            try:
                content = File(self.settings.price_file).read()
                cache = convert.json2value(content, flexible=False, paths=False)
            except Exception, e:
                cache = DictList()

        most_recents = qb.run({
            "from": cache,
            "edges": ["instance_type", "availability_zone"],
            "select": {"value": "timestamp", "aggregate": "max"}
        }).data

        zones = self._get_valid_availability_zones()
        prices = set(cache)
        with Timer("Get pricing from AWS"):
            for instance_type in self.settings.utility.keys():
                for zone in zones:
                    if most_recents:
                        most_recent = most_recents[{
                            "instance_type": instance_type,
                            "availability_zone": zone
                        }].timestamp
                        if most_recent == None:
                            start_at = Date.today() - WEEK
                        else:
                            start_at = Date(most_recent)
                    else:
                        start_at = Date.today() - WEEK

                    if DEBUG_PRICING:
                        Log.note("get pricing for {{instance_type}} starting at {{start_at}}",
                            instance_type=instance_type,
                            start_at=start_at
                        )

                    next_token = None
                    while True:
                        resultset = self.ec2_conn.get_spot_price_history(
                            product_description="Linux/UNIX (Amazon VPC)",
                            instance_type=instance_type,
                            availability_zone=zone,
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

        with Timer("Save prices to file"):
            File(self.settings.price_file).write(convert.value2json(prices))
        return prices


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
    "bad-parameters",
    "canceled-before-fulfillment",
    "instance-terminated-by-user"
}
PENDING_STATUS_CODES = {
    "pending-evaluation",
    "pending-fulfillment"
}
PROBABLY_NOT_FOR_A_WHILE = {
    "az-group-constraint",
    "price-too-low"
}
RUNNING_STATUS_CODES = {
    "fulfilled",
    "request-canceled-and-instance-running"
}


def main():
    try:
        settings = startup.read_settings()
        settings.run_interval = Duration(settings.run_interval)
        Log.start(settings.debug)
        with SingleInstance(flavor_id=settings.args.filename):
            for u in settings.utility:
                u.discount = coalesce(u.discount, 0)
                # MARKUP drives WITH EXPECTED device MAPPING
                num_ephemeral_volumes = ephemeral_storage[u.instance_type]["num"]
                for i, d in enumerate(d for d in u.drives if not d.device):
                    letter = convert.ascii2char(98 + num_ephemeral_volumes + i)
                    d.device = "/dev/xvd" + letter

            settings.utility = UniqueIndex(["instance_type"], data=settings.utility)
            instance_manager = new_instance(settings.instance)
            m = SpotManager(instance_manager, settings=settings)
            m.update_spot_requests(instance_manager.required_utility())

            if m.watcher:
                m.watcher.join()
    except Exception, e:
        Log.warning("Problem with spot manager", e)
    finally:
        Log.stop()
        MAIN_THREAD.stop()


ephemeral_storage = {
    "c1.medium": {"num": 1, "size": 350},
    "c1.xlarge": {"num": 4, "size": 420},
    "c3.2xlarge": {"num": 2, "size": 80},
    "c3.4xlarge": {"num": 2, "size": 160},
    "c3.8xlarge": {"num": 2, "size": 320},
    "c3.large": {"num": 2, "size": 16},
    "c3.xlarge": {"num": 2, "size": 40},
    "c4.2xlarge": {"num": 0, "size": 0},
    "c4.4xlarge": {"num": 0, "size": 0},
    "c4.8xlarge": {"num": 0, "size": 0},
    "c4.large": {"num": 0, "size": 0},
    "c4.xlarge": {"num": 0, "size": 0},
    "cc2.8xlarge": {"num": 4, "size": 840},
    "cg1.4xlarge": {"num": 2, "size": 840},
    "cr1.8xlarge": {"num": 2, "size": 120},
    "d2.2xlarge": {"num": 6, "size": 2000},
    "d2.4xlarge": {"num": 12, "size": 2000},
    "d2.8xlarge": {"num": 24, "size": 2000},
    "d2.xlarge": {"num": 3, "size": 2000},
    "g2.2xlarge": {"num": 1, "size": 60},
    "hi1.4xlarge": {"num": 2, "size": 1024},
    "hs1.8xlarge": {"num": 24, "size": 2000},
    "i2.2xlarge": {"num": 2, "size": 800},
    "i2.4xlarge": {"num": 4, "size": 800},
    "i2.8xlarge": {"num": 8, "size": 800},
    "i2.xlarge": {"num": 1, "size": 800},
    "m1.large": {"num": 2, "size": 420},
    "m1.medium": {"num": 1, "size": 410},
    "m1.small": {"num": 1, "size": 160},
    "m1.xlarge": {"num": 4, "size": 420},
    "m2.2xlarge": {"num": 1, "size": 850},
    "m2.4xlarge": {"num": 2, "size": 840},
    "m2.xlarge": {"num": 1, "size": 420},
    "m3.2xlarge": {"num": 2, "size": 80},
    "m3.large": {"num": 1, "size": 32},
    "m3.medium": {"num": 1, "size": 4},
    "m3.xlarge": {"num": 2, "size": 40},
    "r3.2xlarge": {"num": 1, "size": 160},
    "r3.4xlarge": {"num": 1, "size": 320},
    "r3.8xlarge": {"num": 2, "size": 320},
    "r3.large": {"num": 1, "size": 32},
    "r3.xlarge": {"num": 1, "size": 80},
    "t1.micro": {"num": 0, "size": 0},
    "t2.medium": {"num": 0, "size": 0},
    "t2.micro": {"num": 0, "size": 0},
    "t2.small": {"num": 0, "size": 0}
}

if __name__ == "__main__":
    main()
