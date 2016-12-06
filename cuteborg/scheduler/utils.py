import calendar

from datetime import datetime, timedelta

from .. import config


def align_to_minutes(timestamp, nminutes):
    """
    Align the given `timestamp` to `nminutes`.

    The alignment base is determined by the following rules, in order:

    * If 60 is divisible by `nminutes`, the timestamp is aligned based on the
      hour.

    * If 1440 is divisible by `nminutes`, the timestamp is aligned based on the
      day.

    * Otherwise, the timestamp is aligned based on the UNIX epoch
      (1970-01-01T01:00:00Z).

    `timestamp` must not have a timezone attached, so that timedelta operations
    work. It is assumed to be in UTC.
    """
    if timestamp.tzinfo is not None:
        raise ValueError(
            "align_to_minutes needs naive datetime"
        )

    if 60 % nminutes == 0:
        # nminutes can be aligned by hour
        timestamp = timestamp.replace(
            minute=timestamp.minute // nminutes * nminutes,
            second=0,
            microsecond=0,
        )

    elif 1440 % nminutes == 0:
        # nminutes can be aligned by day
        minute_of_day = timestamp.hour * 60 + timestamp.minute
        hour, minute = divmod(
            minute_of_day // nminutes * nminutes,
            60,
        )

        timestamp = timestamp.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
        )

    else:
        # we need to operate on UNIX epoch
        ts = calendar.timegm(timestamp.timetuple())
        mod = nminutes*60
        ts = ts // mod * mod
        timestamp = datetime.utcfromtimestamp(ts)

    return timestamp


def align_to_hours(timestamp, nhours):
    """
    Align the given `timestamp` to `nhours`.

    If 24 is divisible by `nhours`, the timestamp is aligned based on the day.
    Otherwise, it is based on the UNIX epoch (1970-01-01T01:00:00Z).

    `timestamp` must not have a timezone attached, so that timedelta operations
    work. It is assumed to be in UTC.
    """
    if timestamp.tzinfo is not None:
        raise ValueError(
            "align_to_hours needs naive datetime"
        )

    if 24 % nhours == 0:
        # nhours can be aligned by day
        timestamp = timestamp.replace(
            hour=timestamp.hour // nhours * nhours,
            minute=0,
            second=0,
            microsecond=0,
        )
    else:
        # we need to operate on UNIX epoch
        ts = calendar.timegm(timestamp.timetuple())
        mod = nhours*3600
        ts = ts // mod * mod
        timestamp = datetime.utcfromtimestamp(ts)

    return timestamp


def align_to_days(timestamp, ndays):
    """
    Align the given `timestamp` to `ndays`.

    Day alignment is always relative to the UNIX epoch (1970-01-01T01:00:00Z).

    `timestamp` must not have a timezone attached, so that timedelta operations
    work. It is assumed to be in UTC.
    """
    if timestamp.tzinfo is not None:
        raise ValueError(
            "align_to_days needs naive datetime"
        )

    # we need to operate on UNIX epoch
    ts = calendar.timegm(timestamp.timetuple())
    mod = ndays*86400
    ts = ts // mod * mod
    timestamp = datetime.utcfromtimestamp(ts)

    return timestamp


def align_to_weeks(timestamp, nweeks):
    """
    Align the given `timestamp` to `nweeks`.

    Week alignment is always relative to the first monday of the UNIX epoch
    (1970-01-05T00:00:00Z).

    `timestamp` must not have a timezone attached, so that timedelta operations
    work. It is assumed to be in UTC.
    """
    if timestamp.tzinfo is not None:
        raise ValueError(
            "align_to_days needs naive datetime"
        )

    mod = nweeks*7*86400

    # we need to operate on UNIX epoch
    ts = calendar.timegm(timestamp.timetuple())
    # move back four days to end up at the actual epoch
    ts -= (4*86400)
    ts = ts // mod * mod
    ts += (4*86400)
    timestamp = datetime.utcfromtimestamp(ts)

    return timestamp


def align_to_months(timestamp, nmonths):
    """
    Align the given `timestamp` to `nmonths`.

    If 12 is divisible by `nmonths`, the alignment is based on the year.
    Otherwise, it is based on the UNIX epoch (1970-01-01T01:00:00Z).

    `timestamp` must not have a timezone attached, so that timedelta operations
    work. It is assumed to be in UTC.
    """
    if timestamp.tzinfo is not None:
        raise ValueError(
            "align_to_months needs naive datetime"
        )

    if 12 % nmonths == 0:
        timestamp = timestamp.replace(
            month=((timestamp.month-1) // nmonths * nmonths)+1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    else:
        months_since_epoch = (timestamp.year - 1970) * 12 + timestamp.month-1
        months_since_epoch = months_since_epoch // nmonths * nmonths

        year, month = divmod(months_since_epoch, 12)

        timestamp = timestamp.replace(
            year=1970+year,
            month=month+1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    return timestamp


def align_to_years(timestamp, nyears):
    """
    Align the given `timestamp` to `nyears`.

    Year alignment is always based on the UNIX epoch (1970-01-01T01:00:00Z).

    `timestamp` must not have a timezone attached, so that timedelta operations
    work. It is assumed to be in UTC.
    """
    if timestamp.tzinfo is not None:
        raise ValueError(
            "align_to_months needs naive datetime"
        )

    timestamp = timestamp.replace(
        year=(timestamp.year-1970) // nyears * nyears + 1970,
        month=1,
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    return timestamp


def align_to_interval(timestamp, interval_step, interval_unit):
    """
    Align `timestamp` to a given interval.

    :param timestamp: Timestamp to align
    :type timestamp: :class:`datetime.datetime`
    :param interval_step: Length of the interval, in units of `interval_unit`.
    :type interval_step: :class:`int`
    :param interval_unit: Step size of the interval
    :type interval_unit: :class:`~.IntervalUnit`
    :raises ValueError: If `timestamp` is not a naive
                        :class:`~datetime.datetime`.
    :return: Aligned timestamp
    :rtype: :class:`datetime.datetime`
    """

    if timestamp.tzinfo is not None:
        raise ValueError(
            "align_to_interval needs naive datetime"
        )

    impl = {
        config.IntervalUnit.MINUTE: align_to_minutes,
        config.IntervalUnit.HOUR: align_to_hours,
        config.IntervalUnit.DAY: align_to_days,
        config.IntervalUnit.WEEK: align_to_weeks,
        config.IntervalUnit.MONTH: align_to_months,
        config.IntervalUnit.YEAR: align_to_years,
    }[interval_unit]

    return impl(timestamp, interval_step)


def step_interval(timestamp, interval_step, interval_unit):
    """
    Step a timestamp aligned to the interval by one interval.

    :param timestamp: Timestamp aligned to the interval
    :type timestamp: :class:`datetime.datetime`
    :param interval_step: Length of the interval, in units of `interval_unit`.
    :type interval_step: :class:`int`
    :param interval_unit: Unit of the interval steps.
    :type interval_unit: :class:`~.IntervalUnit`
    :raises ValueError: If `interval_unit` is invalid.
    :raises ValueError: If `timestamp` is not a naive
                        :class:`~datetime.datetime`.
    :raises ValueError: If the resulting timestamp is not valid; this can
                        happen if `timestamp` is not aligned to the interval.
    :return: Timestamp at the beginning of the next interval.
    :rtype: :class:`datetime.datetime`

    .. note::

       The `timestamp` must already be aligned. If it isn’t aligned, the
       resulting timestamp is also not aligned.

       Also, in the case of some interval units, it is possible that the
       resulting timestamp is invalid, thus raising a :class:`ValueError`.
    """

    if timestamp.tzinfo is not None:
        raise ValueError(
            "step_interval needs naive datetime"
        )

    if interval_unit == config.IntervalUnit.MINUTE:
        return timestamp + timedelta(seconds=60*interval_step)

    elif interval_unit == config.IntervalUnit.HOUR:
        return timestamp + timedelta(seconds=3600*interval_step)

    elif interval_unit == config.IntervalUnit.DAY:
        return timestamp + timedelta(days=interval_step)

    elif interval_unit == config.IntervalUnit.WEEK:
        return timestamp + timedelta(days=7*interval_step)

    elif interval_unit == config.IntervalUnit.MONTH:
        months_total = timestamp.year * 12 + timestamp.month
        months_total += interval_step
        year, month = divmod(months_total, 12)
        return timestamp.replace(
            year=year,
            month=month,
        )

    elif interval_unit == config.IntervalUnit.YEAR:
        return timestamp.replace(
            year=timestamp.year+interval_step,
        )

    raise ValueError(
        "invalid interval_unit: {!r}".format(interval_unit)
    )
