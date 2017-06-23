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

from pyLibrary.queries import jx

from mo_dots import wrap, Null
from mo_files import File
from mo_logs import Log, strings
from mo_math import Math

def unquote(value):
    if not value:
        return Null

    try:
        return int(value)
    except Exception:
        pass

    try:
        return float(value)
    except Exception:
        pass

    return value

tab_data = File("resources/EC2.csv").read()
lines = map(strings.trim, tab_data.split("\n"))
header = lines[0].split(",")
rows = [r.split(",") for r in lines[1:] if r]
data = wrap([{h: unquote(r[c]) for c, h in enumerate(header)} for r in rows])


for d in data:
    d.utility = Math.min(d.memory, d.storage/50, 60)
    d.drives["$ref"] = "#" + unicode(d.num_drives) + "_ephemeral_drives"
    d.discount = 0

Log.note("{{data|json(False)}}", data=[d for d in data if d.utility])

Log.note("{{data|json}}", data={d.instance_type: {"num": d.num_drives, "size": d.storage} for d in jx.sort(data, "instance_type")})
