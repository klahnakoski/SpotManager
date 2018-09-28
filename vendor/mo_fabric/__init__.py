# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os
import sys
from datetime import datetime

from fabric2 import Config
from fabric2 import Connection as _Connection

from mo_dots import set_default, unwrap, wrap
from mo_files import File, TempFile
from mo_future import text_type
from mo_kwargs import override
from mo_logs import Log, exceptions, machine_metadata


class Connection(object):

    @override
    def __init__(
        self,
        host,
        user=None,
        port=None,
        config=None,
        gateway=None,
        forward_agent=None,
        connect_timeout=None,
        connect_kwargs=None,
        inline_ssh_env=None,
        key_filename=None,  # part of connect_kwargs
        kwargs=None
    ):
        connect_kwargs = set_default({}, connect_kwargs, {"key_filename": File(key_filename).abspath})
        config = Config(**unwrap(set_default({}, config, {"overrides": {"run": {
            # "hide": True,
            "err_stream": LogStream(host, "stderr"),
            "out_stream": LogStream(host, "stdout")
        }}})))

        self.conn = _Connection(
            host,
            user,
            port,
            config,
            gateway,
            forward_agent,
            connect_timeout,
            connect_kwargs,
            inline_ssh_env
        )
        result = self.conn.run("pwd")
        self.cwd = result.stdout.split("\n")[0]

    def exists(self, path):
        with TempFile() as t:
            try:
                result = self.conn.get(path, t.abspath)
                return t.exists
            except IOError:
                return False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.conn.close()

    def __getattr__(self, item):
        return getattr(self.conn, item)


EMPTY = str("")
CR = str("\n")


class LogStream(object):

    def __init__(self, name, type):
        self.name = name
        self.type = type
        self.part_line = EMPTY

    def write(self, value):
        lines = value.split(CR)
        if len(lines) == 1:
            self.part_line += lines[0]
            return

        prefix = self.part_line
        for line in lines[0:-1]:
            note(u"{{name}} ({{type}}): {{line}}", name=self.name, type=self.type, line=prefix + line)
            prefix = EMPTY
        self.part_line = lines[-1]

    def flush(self):
        pass


def note(
    template,
    **params
):
    if not isinstance(template, text_type):
        Log.error("Log.note was expecting a unicode template")

    if len(template) > 10000:
        template = template[:10000]

    log_params = wrap({
        "template": template,
        "params": params,
        "timestamp": datetime.utcnow(),
        "machine": machine_metadata,
        "context": exceptions.NOTE
    })

    if not template.startswith("\n") and template.find("\n") > -1:
        template = "\n" + template

    if Log.trace:
        log_template = "{{machine.name}} (pid {{machine.pid}}) - {{timestamp|datetime}} - {{thread.name}} - \"{{location.file}}:{{location.line}}\" ({{location.method}}) - " + template.replace("{{", "{{params.")
        f = sys._getframe(1)
        log_params.location = {
            "line": f.f_lineno,
            "file": text_type(f.f_code.co_filename.split(os.sep)[-1]),
            "method": text_type(f.f_code.co_name)
        }
    else:
        log_template = "{{timestamp|datetime}} - " + template.replace("{{", "{{params.")

    Log.main_log.write(log_template, log_params)
