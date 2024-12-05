"""
********************************************************************************
* Name: util.py
* Author: nswain
* Created On: November 29, 2018
* Copyright: (c) Aquaveo 2018
********************************************************************************
"""

from string import Template


class DeltaTemplate(Template):
    delimiter = "%"


def strfdelta(tdelta, fmt):
    """
    Converts the given duration of delta time into H:M:S format.

    Args:
        tdelta(double): duration in time delta.
        fmt(str): duration type

    Returns:
        str: formatted delta time duration value.
    """
    d = {}
    hours, rem = divmod(tdelta.total_seconds(), 3600)
    minutes, seconds = divmod(rem, 60)
    d["H"] = "{:02}".format(int(hours))
    d["M"] = "{:02}".format(int(minutes))
    d["S"] = "{:02}".format(round(seconds))
    t = DeltaTemplate(fmt)
    return t.substitute(**d)
