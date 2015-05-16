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
from datetime import date, datetime
from decimal import Decimal
from types import NoneType
from pyLibrary.dot import wrap, unwrap, Dict, Null, NullType, get_attr

_get = object.__getattribute__
_set = object.__setattr__
WRAPPED_CLASSES = set()

class DictObject(dict):

    def __init__(self, obj):
        dict.__init__(self)
        _set(self, "_obj", obj)

    def __getattr__(self, item):
        obj = _get(self, "_obj")
        try:
            output = _get(obj, item)
        except Exception, _:
            try:
                output = obj[item]
            except Exception, _:
                from pyLibrary.debugs.logs import Log
                Log.error(
                    "Can not find {{item|quote}} in {{type}}",
                    item=item,
                    type=obj.__class__.__name__
                )

        if output == None:
            return None   # So we allow `is` compare to `None`
        return object_wrap(output)

    def __setattr__(self, key, value):
        _get(self, "_dict")[key] = value

    def __getitem__(self, item):
        obj = _get(self, "_obj")
        output = get_attr(obj, item)
        return object_wrap(output)

    def keys(self):
        obj = _get(self, "_obj")
        try:
            return obj.__dict__.keys()
        except Exception, e:
            raise e

    def items(self):
        obj = _get(self, "_obj")
        try:
            return obj.__dict__.items()
        except Exception, e:
            raise e

    def __str__(self):
        obj = _get(self, "_obj")
        return str(obj)

    def __len__(self):
        obj = _get(self, "_obj")
        return len(obj)

    def __call__(self, *args, **kwargs):
        obj = _get(self, "_obj")
        return obj(*args, **kwargs)


def object_wrap(value):
    if value == None:
        return None   # So we allow `is None`
    elif isinstance(value, (basestring, int, float, Decimal, datetime, date, Dict, DictList, NullType, NoneType)):
        return value
    else:
        return DictObject(value)


class DictClass(object):
    """
    ALLOW INSTANCES OF class_ TO ACK LIKE dicts
    ALLOW CONSTRUCTOR TO ACCEPT @use_settings
    """

    def __init__(self, class_):
        WRAPPED_CLASSES.add(class_)
        self.class_ = class_
        self.constructor = class_.__init__

    def __call__(self, *args, **kwargs):
        settings = wrap(kwargs).settings

        params = self.constructor.func_code.co_varnames[1:self.constructor.func_code.co_argcount]
        if not self.constructor.func_defaults:
            defaults = {}
        else:
            defaults = {k: v for k, v in zip(reversed(params), reversed(self.constructor.func_defaults))}

        ordered_params = dict(zip(params, args))

        output = self.class_(**params_pack(params, ordered_params, kwargs, settings, defaults))
        return DictObject(output)


def params_pack(params, *args):
    settings = {}
    for a in args:
        for k, v in a.items():
            k = unicode(k)
            if k in settings:
                continue
            settings[k] = v

    output = {str(k): unwrap(settings[k]) for k in params if k in settings}
    return output


from pyLibrary.dot import DictList
