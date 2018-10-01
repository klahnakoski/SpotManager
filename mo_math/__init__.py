# encoding: utf-8
#
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

import __builtin__
import math

from mo_dots import Null, coalesce


class Math(object):
    """
    MATH FUNCTIONS THAT ASSUME None IMPLY *NOT APPLICABLE* RATHER THAN *MISSING*
    LET "." BE SOME OPERATOR (+, -, *, etc)
    a.None == None
    None.a == None
    .None == None
    func(None, *kwargs)) == None
    """


    @staticmethod
    def bayesian_add(*args):
        a = args[0]
        if a >= 1 or a <= 0:
            from mo_logs import Log
            Log.error("Only allowed values *between* zero and one")

        for b in args[1:]:
            if b == None:
                continue
            if b >= 1 or b <= 0:
                from mo_logs import Log
                Log.error("Only allowed values *between* zero and one")
            a = a * b / (a * b + (1 - a) * (1 - b))

        return a

    @staticmethod
    def bayesian_subtract(a, b):
        return Math.bayesian_add(a, 1 - b)


    @staticmethod
    def abs(v):
        if v == None:
            return Null
        return abs(v)

    @staticmethod
    def pow(n, p):
        if n == None or p == None:
            return None
        return math.pow(n, p)

    @staticmethod
    def exp(v):
        if v == None:
            return Null
        return math.exp(v)

    @staticmethod
    def log(v, base=None):
        try:
            if v == None:
                return Null
            if v == 0.0:
                return -float("inf")
            if base == None:
                return math.log(v)
            return math.log(v, base)
        except Exception as e:
            from mo_logs import Log
            Log.error("error in log")


    @staticmethod
    def log10(v):
        try:
            return math.log(v, 10)
        except Exception as e:
            return Null

    # FOR GOODNESS SAKE - IF YOU PROVIDE A METHOD abs(), PLEASE PROVIDE ITS COMPLEMENT
    # x = abs(x)*sign(x)
    # FOUND IN numpy, BUT WE USUALLY DO NOT NEED TO BRING IN A BIG LIB FOR A SIMPLE DECISION
    @staticmethod
    def sign(v):
        if v == None:
            return Null
        if v < 0:
            return -1
        if v > 0:
            return +1
        return 0


    @staticmethod
    def is_number(s):
        if s is True or s is False or s == None:
            return False

        try:
            s = float(s)
            return not math.isnan(s)
        except Exception:
            return False

    @staticmethod
    def is_nan(s):
        return s==None or math.isnan(s)

    @staticmethod
    def is_finite(s):
        try:
            f = float(s)
            if abs(f) == float("+inf"):
                return False
            return True
        except Exception:
            return False

    @staticmethod
    def is_hex(value):
        try:
            int(value, 16)
            return True
        except Exception:
            return False

    @staticmethod
    def is_integer(s):
        if s is True or s is False:
            return False

        try:
            if float(s) == round(float(s), 0):
                return True
            return False
        except Exception:
            return False

    @staticmethod
    def round(value, decimal=7, digits=None):
        """
        ROUND TO GIVEN NUMBER OF DIGITS, OR GIVEN NUMBER OF DECIMAL PLACES
        decimal - NUMBER OF SIGNIFICANT DIGITS (LESS THAN 1 IS INVALID)
        digits - NUMBER OF DIGITS AFTER DECIMAL POINT (NEGATIVE IS VALID)
        """
        if value == None:
            return None
        else:
            value = float(value)

        if digits != None:
            if digits <= 0:
                if value == 0:
                    return int(__builtin__.round(value, digits))
                try:
                    m = pow(10, math.ceil(math.log10(abs(value))))
                    return int(__builtin__.round(value / m, digits) * m)
                except Exception as e:
                    from mo_logs import Log

                    Log.error("not expected", e)
            else:
                if value == 0:
                    return __builtin__.round(value, digits)
                try:
                    m = pow(10, math.ceil(math.log10(abs(value))))
                    return __builtin__.round(value / m, digits) * m
                except Exception as e:
                    from mo_logs import Log
                    Log.error("not expected", e)
        elif decimal <= 0:
            return int(__builtin__.round(value, decimal))
        else:
            return __builtin__.round(value, decimal)


    @staticmethod
    def floor(value, mod=1):
        """
        x == Math.floor(x, a) + Math.mod(x, a)  FOR ALL a, x
        RETURN None WHEN GIVEN INVALID ARGUMENTS
        """
        if value == None:
            return None
        elif mod <= 0:
            return None
        elif mod == 1:
            return int(math.floor(value))
        elif Math.is_integer(mod):
            return int(math.floor(value/mod))*mod
        else:
            return math.floor(value/mod)*mod



    @staticmethod
    def mod(value, mod=1):
        """
        RETURN NON-NEGATIVE MODULO
        RETURN None WHEN GIVEN INVALID ARGUMENTS
        """
        if value == None:
            return None
        elif mod <= 0:
            return None
        elif value < 0:
            return (value % mod + mod) % mod
        else:
            return value % mod



    # RETURN A VALUE CLOSE TO value, BUT WITH SHORTER len(unicode(value))<len(unicode(value)):
    @staticmethod
    def approx_str(value):
        v = unicode(value)
        d = v.find(".")
        if d == -1:
            return value

        if Math.round(value) == value:
            return int(value)

        i = v.find("9999", d)
        if i == -1:
            i = v.find("0000", d)
            if i == -1:
                return value

        return Math.round(value, decimal=i - d - 1)

    @staticmethod
    def ceiling(value, mod=1):
        """
        RETURN SMALLEST INTEGER GREATER THAN value
        """
        if value == None:
            return None
        mod = int(mod)

        v = int(math.floor(value + mod))
        return v - (v % mod)

    @staticmethod
    def min(*values):
        return MIN(values)

    @staticmethod
    def range(start, stop, interval):
        i = start
        while i<stop:
            yield i
            i+=interval

    @staticmethod
    def max(*values):
        return MAX(values)


def COUNT(values):
    count = 0
    for v in values:
        if v != None:
            count += 1
    return count


def MIN(values, *others):

    if others:
        from mo_logs import Log
        Log.warning("Calling wrong")
        return MIN([values] + list(others))

    output = None
    for v in values:
        if v == None:
            continue
        elif output == None or v < output:
            output = v
        else:
            pass
    return output


def MAX(values, *others):

    if others:
        from mo_logs import Log
        Log.warning("Calling wrong")
        return MAX([values] + list(others))

    output = Null
    for v in values:
        if v == None:
            continue
        elif output == None or v > output:
            output = v
        else:
            pass
    return output


def SUM(values):
    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output += v
    return output


def PRODUCT(values, *others):
    if len(others) > 0:
        from mo_logs import Log
        Log.error("no longer accepting args, use a single list")

    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output *= v
    return output


def AND(values, *others):
    if len(others) > 0:
        from mo_logs import Log
        Log.error("no longer accepting args, use a single list")

    for v in values:
        if v == None:
            continue
        if not v:
            return False
    return True


def OR(values, *others):
    if len(others) > 0:
        from mo_logs import Log
        Log.error("no longer accepting args, use a single list")

    for v in values:
        if v == None:
            continue
        if v:
            return True
    return False


def UNION(values, *others):
    if len(others) > 0:
        from mo_logs import Log
        Log.error("no longer accepting args, use a single list")

    output = set()
    for v in values:
        if values == None:
            continue
        if isinstance(v, (list, set)):
            output.update(v)
            continue
        else:
            output.add(v)
    return output


def INTERSECT(values, *others):
    if len(others) > 0:
        from mo_logs import Log
        Log.error("no longer accepting args, use a single list")

    output = set(values[0])
    for v in values[1:]:
        output -= set(v)
        if not output:
            return output  # EXIT EARLY
    return output


def almost_equal(first, second, digits=None, places=None, delta=None):
    try:
        if first == second:
            return True

        if delta is not None:
            if abs(first - second) <= delta:
                return True
        else:
            places = coalesce(places, digits, 18)
            diff = math.log10(abs(first - second))
            if diff < Math.ceiling(math.log10(first)) - places:
                return True

        return False
    except Exception as e:
        from mo_logs import Log
        Log.error("problem comparing", cause=e)

from mo_math import stats
_ = stats
