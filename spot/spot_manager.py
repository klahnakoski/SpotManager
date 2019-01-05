# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division, unicode_literals

from copy import copy

import boto
import boto.ec2
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.ec2.networkinterface import NetworkInterfaceCollection, NetworkInterfaceSpecification
from boto.utils import ISO8601
import boto.vpc

from jx_python import jx
from jx_python.containers.list_usingPythonList import ListContainer
from mo_collections import UniqueIndex
from mo_dots import Data, FlatList, coalesce, listwrap, unwrap, wrap, Null
from mo_dots.objects import datawrap
from mo_files import File
from mo_future import text_type
from mo_json import value2json
from mo_kwargs import override
from mo_logs import Except, Log, constants, startup
from mo_logs.startup import SingleInstance
from mo_math import MAX, Math, SUM
from mo_threads import Lock, Signal, Thread, Till
from mo_threads.threads import MAIN_THREAD
from mo_times import DAY, Date, Duration, HOUR, MINUTE, SECOND, Timer, WEEK
from pyLibrary import convert
from pyLibrary.meta import cache, new_instance

ENABLE_SIDE_EFFECTS = True
DEBUG_PRICING = True
TIME_FROM_RUNNING_TO_LOGIN = 7 * MINUTE
ERROR_ON_CALL_TO_SETUP = "Problem with setup()"
DELAY_BEFORE_SETUP = 1 * MINUTE  # PROBLEM WITH CONNECTING ONLY HAPPENS WITH BIGGER ES MACHINES
CAPACITY_NOT_AVAILABLE_RETRY = Duration("day")  # SOME MACHINES ARE NOT AVAILABLE


class SpotManager(object):
    @override
    def __init__(self, instance_manager, disable_prices=False, kwargs=None):
        self.settings = kwargs
        self.instance_manager = instance_manager
        aws_args = dict(
            region_name=kwargs.aws.region,
            aws_access_key_id=unwrap(kwargs.aws.aws_access_key_id),
            aws_secret_access_key=unwrap(kwargs.aws.aws_secret_access_key)
        )
        self.ec2_conn = boto.ec2.connect_to_region(**aws_args)
        self.vpc_conn = boto.vpc.connect_to_region(**aws_args)
        self.price_locker = Lock()
        self.prices = None
        self.price_lookup = None
        self.no_capacity = {}
        self.no_capacity_file = File(kwargs.price_file).parent / "no capacity.json"
        self.done_making_new_spot_requests = Signal()
        self.net_new_locker = Lock()
        self.net_new_spot_requests = UniqueIndex(("id",))  # SPOT REQUESTS FOR THIS SESSION
        self.watcher = None
        self.active = None

        self.settings.uptime.bid_percentile = coalesce(self.settings.uptime.bid_percentile, self.settings.bid_percentile)
        self.settings.uptime.history = coalesce(Date(self.settings.uptime.history), DAY)
        self.settings.uptime.duration = coalesce(Duration(self.settings.uptime.duration), Date("5minute"))
        self.settings.max_percent_per_type = coalesce(self.settings.max_percent_per_type, 1)

        if ENABLE_SIDE_EFFECTS and instance_manager and instance_manager.setup_required():
            self._start_life_cycle_watcher()
        if not disable_prices:
            self.pricing()

    def update_spot_requests(self):
        spot_requests = self._get_managed_spot_requests()

        # ADD UP THE CURRENT REQUESTED INSTANCES
        all_instances = UniqueIndex("id", data=self._get_managed_instances())
        self.active = active = wrap([r for r in spot_requests if r.status.code in RUNNING_STATUS_CODES | PENDING_STATUS_CODES | PROBABLY_NOT_FOR_A_WHILE | MIGHT_HAPPEN])

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
        utility_required = self.instance_manager.required_utility(current_utility)
        net_new_utility = utility_required - current_utility

        Log.note("have {{current_utility}} utility running; need {{need_utility}} more utility", current_utility=current_utility, need_utility=net_new_utility)

        if remaining_budget < 0:
            remaining_budget, net_new_utility = self.save_money(remaining_budget, net_new_utility)

        if net_new_utility < 0:
            if self.settings.allowed_overage:
                net_new_utility = Math.min(net_new_utility + self.settings.allowed_overage * utility_required, 0)

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
        Till(seconds=3).wait()
        with self.net_new_locker:
            for req in self.net_new_spot_requests:
                req.add_tag("Name", self.settings.ec2.instance.name)

        Log.note("All requests for new utility have been made")
        self.done_making_new_spot_requests.go()

    def add_instances(self, net_new_utility, remaining_budget):
        prices = self.pricing()

        for p in prices:
            if net_new_utility <= 0 or remaining_budget <= 0:
                break

            if p.current_price == None:
                Log.note("{{type}} has no current price",
                    type=p.type.instance_type
                )
                continue

            if self.settings.utility[p.type.instance_type].blacklist or \
                    p.availability_zone in listwrap(self.settings.utility[p.type.instance_type].blacklist_zones):
                Log.note("{{type}} in {{zone}} skipped due to blacklist", type=p.type.instance_type, zone=p.availability_zone)
                continue

            # DO NOT BID HIGHER THAN WHAT WE ARE WILLING TO PAY
            max_acceptable_price = p.type.utility * self.settings.max_utility_price + p.type.discount
            max_bid = Math.min(p.higher_price, max_acceptable_price, remaining_budget)
            min_bid = p.price_80

            if min_bid > max_acceptable_price:
                Log.note(
                    "Price of ${{price}}/hour on {{type}}: Over remaining acceptable price of ${{remaining}}/hour",
                    type=p.type.instance_type,
                    price=min_bid,
                    remaining=max_acceptable_price
                )
                continue
            elif min_bid > remaining_budget:
                Log.note(
                    "Did not bid ${{bid}}/hour on {{type}}: Over budget of ${{remaining_budget}}/hour",
                    type=p.type.instance_type,
                    bid=min_bid,
                    remaining_budget=remaining_budget
                )
                continue
            elif min_bid > max_bid:
                Log.error("not expected")

            naive_number_needed = int(Math.round(float(net_new_utility) / float(p.type.utility), decimal=0))
            limit_total = None
            if self.settings.max_percent_per_type < 1:
                current_count = sum(1 for a in self.active if a.launch_specification.instance_type == p.type.instance_type and a.launch_specification.placement == p.availability_zone)
                all_count = sum(1 for a in self.active if a.launch_specification.placement == p.availability_zone)
                all_count = max(all_count, naive_number_needed)
                limit_total = int(Math.floor((all_count * self.settings.max_percent_per_type - current_count) / (1 - self.settings.max_percent_per_type)))

            num = Math.min(naive_number_needed, limit_total, self.settings.max_requests_per_type)
            if num < 0:
                Log.note(
                    "{{type}} is over {{limit|percent}} of instances, no more requested",
                    limit=self.settings.max_percent_per_type,
                    type=p.type.instance_type
                )
                continue
            elif num == 1:
                min_bid = Math.min(Math.max(p.current_price * 1.1, min_bid), max_acceptable_price)
                price_interval = 0
            else:
                price_interval = Math.min(min_bid / 10, (max_bid - min_bid) / (num - 1))

            for i in range(num):
                bid_per_machine = min_bid + (i * price_interval)
                if bid_per_machine < p.current_price:
                    Log.note(
                        "Did not bid ${{bid}}/hour on {{type}}: Under current price of ${{current_price}}/hour",
                        type=p.type.instance_type,
                        bid=bid_per_machine - p.type.discount,
                        current_price=p.current_price
                    )
                    continue
                if bid_per_machine - p.type.discount > remaining_budget:
                    Log.note(
                        "Did not bid ${{bid}}/hour on {{type}}: Over remaining budget of ${{remaining}}/hour",
                        type=p.type.instance_type,
                        bid=bid_per_machine - p.type.discount,
                        remaining=remaining_budget
                    )
                    continue

                last_no_capacity_message = self.no_capacity.get(p.type.instance_type, Null)
                if last_no_capacity_message > Date.now() - CAPACITY_NOT_AVAILABLE_RETRY:
                    Log.note(
                        "Did not bid on {{type}}: \"No capacity\" last seen at {{last_time|datetime}}",
                        type=p.type.instance_type,
                        last_time=last_no_capacity_message
                    )
                    continue

                try:
                    if self.settings.ec2.request.count == None or self.settings.ec2.request.count != 1:
                        Log.error("Spot Manager can only request machine one-at-a-time")

                    new_requests = self._request_spot_instances(
                        price=bid_per_machine,
                        availability_zone_group=p.availability_zone,
                        instance_type=p.type.instance_type,
                        kwargs=copy(self.settings.ec2.request)
                    )
                    Log.note(
                        "Request {{num}} instance {{type}} in {{zone}} with utility {{utility}} at ${{price}}/hour",
                        num=len(new_requests),
                        type=p.type.instance_type,
                        zone=p.availability_zone,
                        utility=p.type.utility,
                        price=bid_per_machine
                    )
                    net_new_utility -= p.type.utility * len(new_requests)
                    remaining_budget -= (bid_per_machine - p.type.discount) * len(new_requests)
                    with self.net_new_locker:
                        for ii in new_requests:
                            self.net_new_spot_requests.add(ii)
                except Exception as e:
                    Log.warning(
                        "Request instance {{type}} failed because {{reason}}",
                        type=p.type.instance_type,
                        reason=e.message,
                        cause=e
                    )

                    if "Max spot instance count exceeded" in e.message:
                        Log.note("No further spot requests will be attempted.")
                        return net_new_utility, remaining_budget

        return net_new_utility, remaining_budget

    def remove_instances(self, net_new_utility):
        instances = self.running_instances()

        # FIND COMBO THAT WILL SHUTDOWN WHAT WE NEED EXACTLY, OR MORE
        remove_list = []
        for acceptable_error in range(0, 8):
            remaining_utility = -net_new_utility
            remove_list = FlatList()
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
        remove_threads = [
            Thread.run(
                "teardown for " + text_type(i.id),
                self.instance_manager.teardown,
                i
            )
            for i in remove_list
        ]
        for t in remove_threads:
            try:
                t.join()
            except Exception as e:
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
            except Exception as e:
                r.markup = self.price_lookup[r.instance_type, r.placement]
                Log.error("No pricing!!!", e)
        instances = jx.sort(instances, [
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
                if r.status.code in PENDING_STATUS_CODES | PROBABLY_NOT_FOR_A_WHILE | MIGHT_HAPPEN:
                    remove_spot_requests.append(r.id)
                    net_new_utility += self.settings.utility[r.launch_specification.instance_type].utility
                    remaining_budget += r.price

        instances = jx.sort(self.running_instances(), "markup.estimated_value")

        remove_list = wrap([])
        for s in instances:
            if remaining_budget >= 0:
                break
            remove_list.append(s)
            net_new_utility += coalesce(s.markup.type.utility, 0)
            remaining_budget += coalesce(s.request.bid_price, s.markup.price_80, s.markup.current_price)

        if not remove_list:
            return remaining_budget, net_new_utility

        # SEND SHUTDOWN TO EACH INSTANCE
        Log.warning("Shutdown {{instances}} to save money!", instances=remove_list.id)
        # for i in remove_list:
        #     try:
        #         self.instance_manager.teardown(i, False)
        #     except Exception as e:
        #         Log.warning("Teardown of {{id}} failed", id=i.id, cause=e)
        #
        # remove_spot_requests.extend(remove_list.spot_instance_request_id)
        #
        # # TERMINATE INSTANCES
        # self.ec2_conn.terminate_instances(instance_ids=remove_list.id)
        #
        # # TERMINATE SPOT REQUESTS
        # self.ec2_conn.cancel_spot_instance_requests(request_ids=remove_spot_requests)
        return remaining_budget, net_new_utility

    @cache(duration=5 * SECOND)
    def _get_managed_spot_requests(self):
        output = wrap([datawrap(r) for r in self.ec2_conn.get_all_spot_instance_requests() if not r.tags.get("Name") or r.tags.get("Name").startswith(self.settings.ec2.instance.name)])
        return output

    def _get_managed_instances(self):
        requests = UniqueIndex(["instance_id"], data=self._get_managed_spot_requests().filter(lambda r: r.instance_id!=None))
        reservations = self.ec2_conn.get_all_instances()

        output = []
        for res in reservations:
            for instance in res.instances:
                if instance.tags.get('Name', '').startswith(self.settings.ec2.instance.name) and instance._state.name == "running":
                    instance.request = requests[instance.id]
                    output.append(datawrap(instance))
        return wrap(output)

    def _start_life_cycle_watcher(self):
        def life_cycle_watcher(please_stop):
            failed_attempts = Data()
            setup_threads = []
            bad_requests = Data()

            while not please_stop:
                spot_requests = self._get_managed_spot_requests()
                last_get = Date.now()
                instances = wrap({i.id: i for r in self.ec2_conn.get_all_instances() for i in r.instances})
                # INSTANCES THAT REQUIRE SETUP
                time_to_stop_trying = {}
                please_setup = [
                    (i, r) for i, r in [(instances[r.instance_id], r) for r in spot_requests]
                    if i.id and not i.tags.get("Name") and i._state.name == "running" and Date.now() > Date(i.launch_time) + DELAY_BEFORE_SETUP
                ]

                for i, r in please_setup:
                    if not time_to_stop_trying.get(i.id):
                        time_to_stop_trying[i.id] = Date.now() + TIME_FROM_RUNNING_TO_LOGIN
                    if Date.now() > time_to_stop_trying[i.id]:
                        # FAIL TO SETUP AFTER x MINUTES, THEN TERMINATE INSTANCE
                        self.ec2_conn.terminate_instances(instance_ids=[i.id])
                        with self.net_new_locker:
                            self.net_new_spot_requests.remove(r.id)
                        Log.warning("Problem with setup of {{instance_id}}.  Time is up.  Instance TERMINATED!", instance_id=i.id)
                        continue

                    try:
                        p = self.settings.utility[i.instance_type]
                        if p == None:
                            try:
                                self.ec2_conn.terminate_instances(instance_ids=[i.id])
                                with self.net_new_locker:
                                    self.net_new_spot_requests.remove(r.id)
                            finally:
                                Log.error("Can not setup unknown {{instance_id}} of type {{type}}", instance_id=i.id, type=i.instance_type)

                        i.markup = p
                        i.add_tag("Name", self.settings.ec2.instance.name + " (setup)")
                        setup_threads.append((i, r, Thread.run(
                            "setup for " + text_type(i.id),
                            self.instance_manager.setup,
                            i,
                            p
                        )))
                    except Exception as e:
                        i.delete_tags(["Name"])
                        Log.warning("Unexpected failure on startup", instance_id=i.id, cause=e)

                please_join = [(i, r, t) for i, r, t in setup_threads if t.stopped]
                if please_join:
                    Log.note("{{num}} threads have stopped", num=len(please_join))
                for i, r, t in please_join:
                    try:
                        t.join()
                        setup_threads.remove((i, r, t))
                        i.add_tag("Name", self.settings.ec2.instance.name + " (running)")
                        with self.net_new_locker:
                            self.net_new_spot_requests.remove(r.id)
                    except Exception as e:
                        e = Except.wrap(e)
                        i.delete_tags(["Name"])
                        setup_threads.remove((i, r, t))
                        failed_attempts[r.id] += [e]
                        if "Can not setup unknown " in e:
                            Log.warning("Unexpected failure on startup", instance_id=i.id, cause=e)
                        elif ERROR_ON_CALL_TO_SETUP in e:
                            if len(failed_attempts[r.id]) > 2:
                                Log.warning("Problem with setup() of {{instance_id}}", instance_id=i.id, cause=failed_attempts[r.id])
                        else:
                            Log.warning("Unexpected failure on startup", instance_id=i.id, cause=e)

                if Date.now() - last_get > 5 * SECOND:
                    # REFRESH STALE
                    spot_requests = self._get_managed_spot_requests()
                    last_get = Date.now()

                pending = wrap([r for r in spot_requests if r.status.code in PENDING_STATUS_CODES])
                give_up = wrap([r for r in spot_requests if (r.status.code in PROBABLY_NOT_FOR_A_WHILE | TERMINATED_STATUS_CODES) and r.id not in bad_requests])
                ignore = wrap([r for r in spot_requests if r.status.code in MIGHT_HAPPEN])  # MIGHT HAPPEN, BUT NO NEED TO WAIT FOR IT

                if self.done_making_new_spot_requests:
                    with self.net_new_locker:
                        expired = Date.now() - self.settings.run_interval + 2 * MINUTE
                        for ii in list(self.net_new_spot_requests):
                            if Date(ii.create_time) < expired:
                                # SOMETIMES REQUESTS NEVER GET INTO THE MAIN LIST OF REQUESTS
                                self.net_new_spot_requests.remove(ii)

                        for g in ignore:
                            self.net_new_spot_requests.remove(g.id)
                        pending = UniqueIndex(("id",), data=pending)
                        pending = pending | self.net_new_spot_requests

                    if give_up:
                        self.ec2_conn.cancel_spot_instance_requests(request_ids=give_up.id)
                        Log.note("Cancelled spot requests {{spots}}, {{reasons}}", spots=give_up.id, reasons=give_up.status.code)

                        for g in give_up:
                            bad_requests[g.id] += 1
                            if g.id in self.net_new_spot_requests:
                                self.net_new_spot_requests.remove(g.id)
                                if g.status.code == "capacity-not-available":
                                    self.no_capacity[g.launch_specification.instance_type] = Date.now()
                                if g.status.code == "bad-parameters":
                                    self.no_capacity[g.launch_specification.instance_type] = Date.now()
                                    Log.warning("bad parameters while requesting type {{type}}", type=g.launch_specification.instance_type)

                if not pending and not time_to_stop_trying and self.done_making_new_spot_requests and not setup_threads:
                    Log.note("No more pending spot requests")
                    please_stop.go()
                    break
                elif setup_threads:
                    Log.note("waiting for setup of {{num}} instances", num=len(setup_threads))
                elif pending:
                    Log.note("waiting for spot requests: {{pending}}", pending=[p.id for p in pending])

                (Till(seconds=10) | please_stop).wait()

            with Timer("Save no capacity to file"):
                table = [
                    {"instance_type": k, "last_failure": v}
                    for k, v in self.no_capacity.items()
                ]
                self.no_capacity_file.write(value2json(table, pretty=True))

            Log.note("life cycle watcher has stopped")

        # Log.warning("lifecycle watcher is disabled")
        timeout = Till(seconds=self.settings.run_interval.seconds - 60)
        self.watcher = Thread.run("lifecycle watcher", life_cycle_watcher, please_stop=timeout)

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

    @override
    def _request_spot_instances(self, price, availability_zone_group, instance_type, kwargs):
        kwargs.kwargs = None

        # m3 INSTANCES ARE NOT ALLOWED PLACEMENT GROUP
        if instance_type.startswith("m3."):
            kwargs.placement_group = None

        kwargs.network_interfaces = NetworkInterfaceCollection(*(
            NetworkInterfaceSpecification(**i)
            for i in listwrap(kwargs.network_interfaces)
            if self.vpc_conn.get_all_subnets(subnet_ids=i.subnet_id, filters={"availabilityZone": availability_zone_group})
        ))

        if len(kwargs.network_interfaces) == 0:
            Log.error("No network interface specifications found for {{availability_zone}}!", availability_zone=kwargs.availability_zone_group)

        block_device_map = BlockDeviceMapping()

        # GENERIC BLOCK DEVICE MAPPING
        for dev, dev_settings in kwargs.block_device_map.items():
            block_device_map[dev] = BlockDeviceType(
                delete_on_termination=True,
                **dev_settings
            )

        kwargs.block_device_map = block_device_map

        # INCLUDE EPHEMERAL STORAGE IN BlockDeviceMapping
        num_ephemeral_volumes = ephemeral_storage[instance_type]["num"]
        for i in range(num_ephemeral_volumes):
            letter = convert.ascii2char(98 + i)  # START AT "b"
            kwargs.block_device_map["/dev/sd" + letter] = BlockDeviceType(
                ephemeral_name='ephemeral' + text_type(i),
                delete_on_termination=True
            )

        if kwargs.expiration:
            kwargs.valid_until = (Date.now() + Duration(kwargs.expiration)).format(ISO8601)
            kwargs.expiration = None

        # ATTACH NEW EBS VOLUMES
        for i, drive in enumerate(self.settings.utility[instance_type].drives):
            letter = convert.ascii2char(98 + i + num_ephemeral_volumes)
            device = drive.device = coalesce(drive.device, "/dev/sd" + letter)
            d = drive.copy()
            d.path = None  # path AND device PROPERTY IS NOT ALLOWED IN THE BlockDeviceType
            d.device = None
            if d.size:
                kwargs.block_device_map[device] = BlockDeviceType(
                    delete_on_termination=True,
                    **d
                )

        output = list(self.ec2_conn.request_spot_instances(**kwargs))
        return output

    def pricing(self):
        with self.price_locker:
            if self.prices:
                return self.prices

            prices = self._get_spot_prices_from_aws()
            now = Date.now()

            with Timer("processing pricing data"):
                hourly_pricing = jx.run({
                    "from": {
                        # AWS PRICING ONLY SENDS timestamp OF CHANGES, MATCH WITH NEXT INSTANCE
                        "from": prices,
                        "window": [
                            {
                                "name": "expire",
                                "value": {"coalesce": [{"rows": {"timestamp": 1}}, {"date": "eod"}]},
                                "edges": ["availability_zone", "instance_type"],
                                "sort": "timestamp"
                            },
                            {  # MAKE THIS PRICE EFFECTIVE INTO THE PAST, THIS HELPS SPREAD PRICE SPIKES OVER TIME
                                "name": "effective",
                                "value": {"sub": {"timestamp": self.settings.uptime.duration.seconds}}
                            }
                        ]
                    },
                    "edges": [
                        "availability_zone",
                        "instance_type",
                        {
                            "name": "time",
                            "range": {"min": "effective", "max": "expire", "mode": "inclusive"},
                            "allowNulls": False,
                            "domain": {"type": "time", "min": now.floor(HOUR) - self.settings.uptime.history, "max": Date.now().floor(HOUR)+HOUR, "interval": "hour"}
                        }
                    ],
                    "select": [
                        {"value": "price", "aggregate": "max"},
                        {"aggregate": "count"}
                    ],
                    "where": {"gt": {"expire": now.floor(HOUR) - self.settings.uptime.history}},
                    "window": [
                        {
                            "name": "current_price",
                            "value": "rows.last.price",
                            "edges": ["availability_zone", "instance_type"],
                            "sort": "time"
                        }
                    ]
                }).data

                bid80 = jx.run({
                    "from": ListContainer(name=None, data=hourly_pricing),
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
                        {"name": "price_80", "value": "price", "aggregate": "percentile", "percentile": self.settings.uptime.bid_percentile},
                        {"name": "max_price", "value": "price", "aggregate": "max"},
                        {"aggregate": "count"},
                        {"value": "current_price", "aggregate": "one"},
                        {"name": "all_price", "value": "price", "aggregate": "list"}
                    ],
                    "window": [
                        {"name": "estimated_value", "value": {"div": ["type.utility", "price_80"]}},
                        {"name": "higher_price", "value": lambda row, rownum, rows: find_higher(row.all_price, row.price_80)}  # TODO: SUPPORT {"from":"all_price", "where":{"gt":[".", "price_80"]}, "select":{"aggregate":"min"}}
                    ]
                })

                output = jx.sort(bid80.values(), {"value": "estimated_value", "sort": -1})

                self.prices = wrap(output)
                self.price_lookup = UniqueIndex(("type.instance_type", "availability_zone"), data=self.prices)
            return self.prices

    def _get_spot_prices_from_aws(self):
        with Timer("Read no capacity file"):
            try:
                # FILE IS LIST OF {instance_type, last_failure} OBJECTS
                content = self.no_capacity_file.read()
                self.no_capacity = dict(
                    (r.instance_type, r.last_failure)
                    for r in convert.json2value(content, flexible=False, leaves=False)
                )
            except Exception as e:
                self.no_capacity = {}

        with Timer("Read pricing file"):
            try:
                content = File(self.settings.price_file).read()
                cache = convert.json2value(content, flexible=False, leaves=False)
            except Exception as e:
                cache = FlatList()

        cache = ListContainer(name=None, data=cache)
        most_recents = jx.run({
            "from": cache,
            "edges": ["instance_type", "availability_zone"],
            "select": {"value": "timestamp", "aggregate": "max"}
        })

        zones = self._get_valid_availability_zones()
        prices = set(cache)
        with Timer("Get pricing from AWS"):
            for instance_type in self.settings.utility.keys():
                for zone in zones:
                    if cache:
                        most_recent = most_recents[{
                            "instance_type": instance_type,
                            "availability_zone": zone
                        }].timestamp
                        start_at = MAX([Date(most_recent), Date.today() - WEEK])
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
                            product_description=coalesce(self.settings.product, "Linux/UNIX (Amazon VPC)"),
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
                                "timestamp": Date(p.timestamp).unix
                            }))

                        if not next_token:
                            break

        with Timer("Save prices to file"):
            new_prices = jx.filter(prices, {"gte": {"timestamp": {"date": "today-2day"}}})
            def stream():  # IT'S A LOT OF PRICES, STREAM THEM TO FILE
                prefix = "[\n"
                for p in new_prices:
                    yield prefix
                    yield convert.value2json(p)
                    prefix = ",\n"
                yield "]"
            File(self.settings.price_file).write(stream())

        return ListContainer(name="prices", data=prices)


def find_higher(candidates, reference):
    """
    RETURN ONE PRICE HIGHER THAN reference
    """
    output = wrap(sorted(c for c in candidates if c > reference))[0]
    return output


TERMINATED_STATUS_CODES = {
    "marked-for-termination",   # AS GOOD AS DEAD
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
MIGHT_HAPPEN = {
    "az-group-constraint"
}
PROBABLY_NOT_FOR_A_WHILE = {
    "placement-group-constraint",
    "price-too-low"
}


RUNNING_STATUS_CODES = {
    "fulfilled",
    "request-canceled-and-instance-running"
}


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        with SingleInstance(flavor_id=settings.args.filename):
            constants.set(settings.constants)
            settings.run_interval = Duration(settings.run_interval)
            for u in settings.utility:
                u.discount = coalesce(u.discount, 0)
                # MARKUP drives WITH EXPECTED device MAPPING
                num_ephemeral_volumes = ephemeral_storage[u.instance_type]["num"]
                for i, d in enumerate(d for d in u.drives if not d.device):
                    letter = convert.ascii2char(98 + num_ephemeral_volumes + i)
                    d.device = "/dev/xvd" + letter

            settings.utility = UniqueIndex(["instance_type"], data=settings.utility)
            instance_manager = new_instance(settings.instance)
            m = SpotManager(instance_manager, kwargs=settings)

            if ENABLE_SIDE_EFFECTS:
                m.update_spot_requests()

            if m.watcher:
                m.watcher.join()
    except Exception as e:
        Log.warning("Problem with spot manager", cause=e)
    finally:
        Log.stop()
        MAIN_THREAD.stop()


ephemeral_storage = {
    "c1.medium": {"num": 1, "size": 350},
    "c1.xlarge": {"num": 4, "size": 1680},

    "c3.2xlarge": {"num": 2, "size": 160},
    "c3.4xlarge": {"num": 2, "size": 320},
    "c3.8xlarge": {"num": 2, "size": 640},
    "c3.large": {"num": 2, "size": 32},
    "c3.xlarge": {"num": 2, "size": 80},

    "c4.2xlarge": {"num": 0, "size": 0},
    "c4.4xlarge": {"num": 0, "size": 0},
    "c4.8xlarge": {"num": 0, "size": 0},
    "c4.large": {"num": 0, "size": 0},
    "c4.xlarge": {"num": 0, "size": 0},

    "c5.large":{"num": 0, "size": 0},
    "c5.xlarge":{"num": 0, "size": 0},
    "c5.2xlarge":{"num": 0, "size": 0},
    "c5.4xlarge":{"num": 0, "size": 0},
    "c5.9xlarge":{"num": 0, "size": 0},
    "c5.18xlarge":{"num": 0, "size": 0},

    "cc2.8xlarge": {"num": 4, "size": 3360},
    "cg1.4xlarge": {"num": 2, "size": 1680},
    "cr1.8xlarge": {"num": 2, "size": 240},

    "d2.2xlarge": {"num": 6, "size": 12000},
    "d2.4xlarge": {"num": 12, "size": 24000},
    "d2.8xlarge": {"num": 24, "size": 48000},
    "d2.xlarge": {"num": 3, "size": 6000},

    "g2.2xlarge": {"num": 1, "size": 60},
    "g2.8xlarge": {"num": 2, "size": 240},
    "h1.2xlarge": {"num": 1, "size": 2000},
    "h1.4xlarge": {"num": 2, "size": 4000},
    "h1.8xlarge": {"num": 4, "size": 8000},
    "h1.16xlarge": {"num": 8, "size": 16000},

    "hi1.4xlarge": {"num": 2, "size": 2048},
    "hs1.8xlarge": {"num": 24, "size": 48000},
    "i2.2xlarge": {"num": 2, "size": 1600},
    "i2.4xlarge": {"num": 4, "size": 3200},
    "i2.8xlarge": {"num": 8, "size": 6400},
    "i2.xlarge": {"num": 1, "size": 800},

    "i3.16xlarge": {"num": 8, "size": 15200},
    "i3.2xlarge": {"num": 1, "size": 1900},
    "i3.4xlarge": {"num": 2, "size": 3800},
    "i3.8xlarge": {"num": 4, "size": 7600},
    "i3.large": {"num": 1, "size": 475},
    "i3.xlarge": {"num": 1, "size": 950},

    "f1.2xlarge": {"num": 1, "size": 470},
    "f1.4xlarge": {"num": 1, "size": 940},
    "f1.16xlarge": {"num": 4, "size": 940},

    "m3.2xlarge": {"num": 2, "size": 160},
    "m3.large": {"num": 1, "size": 32},
    "m3.medium": {"num": 1, "size": 4},
    "m3.xlarge": {"num": 2, "size": 80},
    "m4.10xlarge": {"num": 0, "size": 0},
    "m4.16xlarge": {"num": 0, "size": 0},
    "m4.2xlarge": {"num": 0, "size": 0},
    "m4.4xlarge": {"num": 0, "size": 0},
    "m4.large": {"num": 0, "size": 0},
    "m4.xlarge": {"num": 0, "size": 0},

    "m5d.large": {"num": 1, "size": 75},
    "m5d.xlarge": {"num": 1, "size": 150},
    "m5d.2xlarge": {"num": 1, "size": 300},
    "m5d.4xlarge": {"num": 2, "size": 300},
    "m5d.12xlarge": {"num": 2, "size": 900},
    "m5d.24xlarge": {"num": 4, "size": 900},

    "p2.16xlarge": {"num": 0, "size": 0},
    "p2.8xlarge": {"num": 0, "size": 0},
    "p2.xlarge": {"num": 0, "size": 0},
    "r3.2xlarge": {"num": 1, "size": 160},
    "r3.4xlarge": {"num": 1, "size": 320},
    "r3.8xlarge": {"num": 2, "size": 640},
    "r3.large": {"num": 1, "size": 32},
    "r3.xlarge": {"num": 1, "size": 80},
    "r4.16xlarge": {"num": 0, "size": 0},
    "r4.2xlarge": {"num": 0, "size": 0},
    "r4.4xlarge": {"num": 0, "size": 0},
    "r4.8xlarge": {"num": 0, "size": 0},
    "r4.large": {"num": 0, "size": 0},
    "r4.xlarge": {"num": 0, "size": 0},

    "r5d.large": {"num": 1, "size": 75},
    "r5d.xlarge": {"num": 1, "size": 150},
    "r5d.2xlarge": {"num": 1, "size": 300},
    "r5d.4xlarge": {"num": 2, "size": 300},
    "r5d.12xlarge": {"num": 2, "size": 900},
    "r5d.24xlarge": {"num": 4, "size": 900},

    "t1.micro": {"num": 0, "size": 0},
    "t2.2xlarge": {"num": 0, "size": 0},
    "t2.large": {"num": 0, "size": 0},
    "t2.medium": {"num": 0, "size": 0},
    "t2.micro": {"num": 0, "size": 0},
    "t2.nano": {"num": 0, "size": 0},
    "t2.small": {"num": 0, "size": 0},
    "t2.xlarge": {"num": 0, "size": 0},

    "c5d.large": {"num": 1, "size": 50},
    "c5d.xlarge": {"num": 1, "size": 100},
    "c5d.2xlarge": {"num": 1, "size": 200},
    "c5d.4xlarge": {"num": 1, "size": 400},
    "c5d.9xlarge": {"num": 1, "size": 900},
    "c5d.18xlarge": {"num": 2, "size": 900},

    "x1e.xlarge": {"num": 1, "size": 120},
    "x1e.2xlarge": {"num": 1, "size": 240},
    "x1e.4xlarge": {"num": 1, "size": 480},
    "x1e.8xlarge": {"num": 1, "size": 960},
    "x1e.16xlarge": {"num": 1, "size": 1920},
    "x1e.32xlarge": {"num": 2, "size": 1920},

    "z1d.large": {"num": 1, "size": 75},
    "z1d.xlarge": {"num": 1, "size": 150},
    "z1d.2xlarge": {"num": 1, "size": 300},
    "z1d.3xlarge": {"num": 1, "size": 450},
    "z1d.6xlarge": {"num": 1, "size": 900},
    "z1d.12xlarge": {"num": 1, "size": 900},

    "x1.16xlarge": {"num": 1, "size": 1920},
    "x1.32xlarge": {"num": 2, "size": 3840}
}

if __name__ == "__main__":
    main()
