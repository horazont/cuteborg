########################################################################
# File name: test_utils.py
# This file is part of: cuteborg
#
# LICENSE
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
########################################################################
import contextlib
import unittest
import unittest.mock

import pytz

import cuteborg.config as config
import cuteborg.scheduler.utils as utils

from datetime import datetime, timedelta


class Testalign_to_minutes(unittest.TestCase):
    DATETIMES = [
        datetime(2016, 12, 2, 9, 17, 26, 432304),
        datetime(2016, 12, 2, 9, 17, 26),
        datetime(2016, 12, 2, 9, 17),
        datetime(2016, 12, 2, 9, 0),
        datetime(2016, 12, 2, 0, 0),
        datetime(2016, 12, 1, 0, 0),
        datetime(2016, 1, 1, 0, 0),
    ]

    def test_reject_timezoned_timestamp(self):
        tz = pytz.timezone("Europe/Berlin")
        dt = tz.normalize(datetime.utcnow().replace(tzinfo=pytz.UTC))
        with self.assertRaisesRegex(
                ValueError,
                "needs naive datetime"):
            utils.align_to_minutes(dt, 2)

    def test_align_based_on_hour(self):
        expected_dts = [
            datetime(2016, 12, 2, 9, 15, 0),
            datetime(2016, 12, 2, 9, 15, 0),
            datetime(2016, 12, 2, 9, 15),
            datetime(2016, 12, 2, 9, 0),
            datetime(2016, 12, 2, 0, 0),
            datetime(2016, 12, 1, 0, 0),
            datetime(2016, 1, 1, 0, 0),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_minutes(
                    given_dt,
                    5,
                ),
                expected_dt,
                given_dt,
            )

    def test_align_based_on_day(self):
        expected_dts = [
            datetime(2016, 12, 2, 9, 12, 0),
            datetime(2016, 12, 2, 9, 12, 0),
            datetime(2016, 12, 2, 9, 12),
            datetime(2016, 12, 2, 8, 56),
            datetime(2016, 12, 2, 0, 0),
            datetime(2016, 12, 1, 0, 0),
            datetime(2016, 1, 1, 0, 0),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_minutes(
                    given_dt,
                    8,
                ),
                expected_dt,
                given_dt,
            )

    def test_align_based_on_epoch(self):
        expected_dts = [
            datetime(2016, 12, 2, 9, 15, 0),
            datetime(2016, 12, 2, 9, 15, 0),
            datetime(2016, 12, 2, 9, 15),
            datetime(2016, 12, 2, 8, 54),
            datetime(2016, 12, 1, 23, 55),
            datetime(2016, 12, 1, 0, 0),
            datetime(2015, 12, 31, 23, 55),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_minutes(
                    given_dt,
                    7,
                ),
                expected_dt,
                given_dt,
            )


class Testalign_to_hours(unittest.TestCase):
    DATETIMES = [
        datetime(2016, 12, 2, 9, 17, 26, 432304),
        datetime(2016, 12, 2, 9, 17, 26),
        datetime(2016, 12, 2, 9, 17),
        datetime(2016, 12, 2, 9, 0),
        datetime(2016, 12, 2, 0, 0),
        datetime(2016, 12, 1, 0, 0),
        datetime(2016, 1, 1, 0, 0),
    ]

    def test_reject_timezoned_timestamp(self):
        tz = pytz.timezone("Europe/Berlin")
        dt = tz.normalize(datetime.utcnow().replace(tzinfo=pytz.UTC))
        with self.assertRaisesRegex(
                ValueError,
                "needs naive datetime"):
            utils.align_to_hours(dt, 2)

    def test_align_to_single_hour(self):
        expected_dts = [
            datetime(2016, 12, 2, 9, 0, 0),
            datetime(2016, 12, 2, 9, 0, 0),
            datetime(2016, 12, 2, 9, 0),
            datetime(2016, 12, 2, 9, 0),
            datetime(2016, 12, 2, 0, 0),
            datetime(2016, 12, 1, 0, 0),
            datetime(2016, 1, 1, 0, 0),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_hours(
                    given_dt,
                    1,
                ),
                expected_dt,
                given_dt,
            )

    def test_align_to_two_hours(self):
        expected_dts = [
            datetime(2016, 12, 2, 8, 0, 0),
            datetime(2016, 12, 2, 8, 0, 0),
            datetime(2016, 12, 2, 8, 0),
            datetime(2016, 12, 2, 8, 0),
            datetime(2016, 12, 2, 0, 0),
            datetime(2016, 12, 1, 0, 0),
            datetime(2016, 1, 1, 0, 0),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_hours(
                    given_dt,
                    2,
                ),
                expected_dt,
                given_dt,
            )

    def test_align_to_five_hours(self):
        expected_dts = [
            datetime(2016, 12, 2, 7, 0, 0),
            datetime(2016, 12, 2, 7, 0, 0),
            datetime(2016, 12, 2, 7, 0),
            datetime(2016, 12, 2, 7, 0),
            datetime(2016, 12, 1, 21, 0),
            datetime(2016, 11, 30, 20, 0),
            datetime(2015, 12, 31, 20, 0),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_hours(
                    given_dt,
                    5,
                ),
                expected_dt,
                given_dt,
            )


class Testalign_to_days(unittest.TestCase):
    DATETIMES = [
        datetime(2016, 12, 2, 9, 17, 26, 432304),
        datetime(2016, 12, 2, 9, 17, 26),
        datetime(2016, 12, 2, 9, 17),
        datetime(2016, 12, 2, 9, 0),
        datetime(2016, 12, 2, 0, 0),
        datetime(2016, 12, 1, 0, 0),
        datetime(2016, 2, 28, 0, 0),
        datetime(2016, 1, 1, 0, 0),
    ]

    def test_reject_timezoned_timestamp(self):
        tz = pytz.timezone("Europe/Berlin")
        dt = tz.normalize(datetime.utcnow().replace(tzinfo=pytz.UTC))
        with self.assertRaisesRegex(
                ValueError,
                "needs naive datetime"):
            utils.align_to_days(dt, 2)

    def test_align_to_five_days_leapyear(self):
        expected_dts = [
            datetime(2016, 11, 30),
            datetime(2016, 11, 30),
            datetime(2016, 11, 30),
            datetime(2016, 11, 30),
            datetime(2016, 11, 30),
            datetime(2016, 11, 30),
            datetime(2016, 2, 24),
            datetime(2015, 12, 31),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_days(
                    given_dt,
                    5,
                ),
                expected_dt,
                given_dt,
            )

    def test_align_to_five_days_nonleapyear(self):
        expected_dts = [
            datetime(2015, 12, 1),
            datetime(2015, 12, 1),
            datetime(2015, 12, 1),
            datetime(2015, 12, 1),
            datetime(2015, 12, 1),
            datetime(2015, 12, 1),
            datetime(2015, 2, 24),
            datetime(2014, 12, 31),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_days(
                    given_dt.replace(year=2015),
                    5,
                ),
                expected_dt,
                given_dt.replace(year=2015),
            )

    def test_align_to_six_days(self):
        expected_dts = [
            datetime(2016, 12, 1),
            datetime(2016, 12, 1),
            datetime(2016, 12, 1),
            datetime(2016, 12, 1),
            datetime(2016, 12, 1),
            datetime(2016, 12, 1),
            datetime(2016, 2, 23),
            datetime(2015, 12, 31),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            self.assertEqual(
                utils.align_to_days(
                    given_dt,
                    6,
                ),
                expected_dt,
                given_dt,
            )


class Testalign_to_weeks(unittest.TestCase):
    DATETIMES = [
        datetime(2016, 12, 2, 9, 17, 26, 432304),
        datetime(2016, 12, 2, 9, 17, 26),
        datetime(2016, 12, 2, 9, 17),
        datetime(2016, 12, 2, 9, 0),
        datetime(2016, 12, 2, 0, 0),
        datetime(2016, 12, 1, 0, 0),
        datetime(2016, 2, 28, 0, 0),
        datetime(2016, 1, 1, 0, 0),
    ]

    def test_reject_timezoned_timestamp(self):
        tz = pytz.timezone("Europe/Berlin")
        dt = tz.normalize(datetime.utcnow().replace(tzinfo=pytz.UTC))
        with self.assertRaisesRegex(
                ValueError,
                "needs naive datetime"):
            utils.align_to_weeks(dt, 2)

    def test_align_to_two_weeks(self):
        expected_dts = [
            datetime(2016, 11, 21),
            datetime(2016, 11, 21),
            datetime(2016, 11, 21),
            datetime(2016, 11, 21),
            datetime(2016, 11, 21),
            datetime(2016, 11, 21),
            datetime(2016, 2, 15),
            datetime(2015, 12, 21),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            result_dt = utils.align_to_weeks(
                given_dt,
                2,
            )

            self.assertEqual(
                result_dt,
                expected_dt,
                given_dt,
            )

            self.assertEqual(
                result_dt.weekday(),
                0,
                "{} did not get aligned to monday".format(
                    given_dt
                )
            )


class Testalign_to_months(unittest.TestCase):
    DATETIMES = [
        datetime(2016, 12, 2, 9, 17, 26, 432304),
        datetime(2016, 12, 2, 9, 17, 26),
        datetime(2016, 12, 2, 9, 17),
        datetime(2016, 12, 2, 9, 0),
        datetime(2016, 12, 2, 0, 0),
        datetime(2016, 12, 1, 0, 0),
        datetime(2016, 3, 28, 0, 0),
        datetime(2016, 2, 28, 0, 0),
        datetime(2016, 1, 1, 0, 0),
    ]

    def test_reject_timezoned_timestamp(self):
        tz = pytz.timezone("Europe/Berlin")
        dt = tz.normalize(datetime.utcnow().replace(tzinfo=pytz.UTC))
        with self.assertRaisesRegex(
                ValueError,
                "needs naive datetime"):
            utils.align_to_months(dt, 2)

    def test_align_to_two_months(self):
        expected_dts = [
            datetime(2016, 11, 1),
            datetime(2016, 11, 1),
            datetime(2016, 11, 1),
            datetime(2016, 11, 1),
            datetime(2016, 11, 1),
            datetime(2016, 11, 1),
            datetime(2016, 3, 1),
            datetime(2016, 1, 1),
            datetime(2016, 1, 1),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            result_dt = utils.align_to_months(
                given_dt,
                2,
            )

            self.assertEqual(
                result_dt,
                expected_dt,
                given_dt,
            )

    def test_align_to_five_months(self):
        expected_dts = [
            datetime(2016, 9, 1),
            datetime(2016, 9, 1),
            datetime(2016, 9, 1),
            datetime(2016, 9, 1),
            datetime(2016, 9, 1),
            datetime(2016, 9, 1),
            datetime(2015, 11, 1),
            datetime(2015, 11, 1),
            datetime(2015, 11, 1),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            result_dt = utils.align_to_months(
                given_dt,
                5,
            )

            self.assertEqual(
                result_dt,
                expected_dt,
                given_dt,
            )


class Testalign_to_years(unittest.TestCase):
    DATETIMES = [
        datetime(2016, 12, 2, 9, 17, 26, 432304),
        datetime(2016, 12, 2, 9, 17, 26),
        datetime(2016, 12, 2, 9, 17),
        datetime(2016, 12, 2, 9, 0),
        datetime(2016, 12, 2, 0, 0),
        datetime(2016, 12, 1, 0, 0),
        datetime(2016, 1, 1, 0, 0),
        datetime(2015, 6, 1, 0, 0),
    ]

    def test_reject_timezoned_timestamp(self):
        tz = pytz.timezone("Europe/Berlin")
        dt = tz.normalize(datetime.utcnow().replace(tzinfo=pytz.UTC))
        with self.assertRaisesRegex(
                ValueError,
                "needs naive datetime"):
            utils.align_to_years(dt, 2)

    def test_align_to_two_years(self):
        expected_dts = [
            datetime(2016, 1, 1),
            datetime(2016, 1, 1),
            datetime(2016, 1, 1),
            datetime(2016, 1, 1),
            datetime(2016, 1, 1),
            datetime(2016, 1, 1),
            datetime(2016, 1, 1),
            datetime(2014, 1, 1),
        ]

        for given_dt, expected_dt in zip(self.DATETIMES, expected_dts):
            result_dt = utils.align_to_years(
                given_dt,
                2,
            )

            self.assertEqual(
                result_dt,
                expected_dt,
                given_dt,
            )


class Testalign_to_interval(unittest.TestCase):
    def setUp(self):
        self.dt = datetime(2016, 1, 2, 3, 4, 5, 6)

    def test_reject_timezoned_timestamp_without_calling_individual_function(
            self):
        tz = pytz.timezone("Europe/Berlin")
        dt = tz.normalize(datetime.utcnow().replace(tzinfo=pytz.UTC))
        with contextlib.ExitStack() as stack:
            align_to_days = stack.enter_context(
                unittest.mock.patch(
                    "cuteborg.scheduler.utils.align_to_days"
                )
            )

            stack.enter_context(
                self.assertRaisesRegex(
                    ValueError,
                    "needs naive datetime")
            )

            utils.align_to_interval(
                dt,
                2,
                config.IntervalUnit.DAY,
            )

        align_to_days.assert_not_called()

    def test_dispatch_to_minute_impl(self):
        with contextlib.ExitStack() as stack:
            align_to_minutes = stack.enter_context(
                unittest.mock.patch(
                    "cuteborg.scheduler.utils.align_to_minutes",
                )
            )

            result = utils.align_to_interval(
                self.dt,
                unittest.mock.sentinel.step,
                config.IntervalUnit.MINUTE,
            )

        align_to_minutes.assert_called_once_with(
            self.dt,
            unittest.mock.sentinel.step,
        )

        self.assertEqual(
            result,
            align_to_minutes(),
        )

    def test_dispatch_to_hour_impl(self):
        with contextlib.ExitStack() as stack:
            align_to_hours = stack.enter_context(
                unittest.mock.patch(
                    "cuteborg.scheduler.utils.align_to_hours",
                )
            )

            result = utils.align_to_interval(
                self.dt,
                unittest.mock.sentinel.step,
                config.IntervalUnit.HOUR,
            )

        align_to_hours.assert_called_once_with(
            self.dt,
            unittest.mock.sentinel.step,
        )

        self.assertEqual(
            result,
            align_to_hours(),
        )

    def test_dispatch_to_day_impl(self):
        with contextlib.ExitStack() as stack:
            align_to_days = stack.enter_context(
                unittest.mock.patch(
                    "cuteborg.scheduler.utils.align_to_days",
                )
            )

            result = utils.align_to_interval(
                self.dt,
                unittest.mock.sentinel.step,
                config.IntervalUnit.DAY,
            )

        align_to_days.assert_called_once_with(
            self.dt,
            unittest.mock.sentinel.step,
        )

        self.assertEqual(
            result,
            align_to_days(),
        )

    def test_dispatch_to_week_impl(self):
        with contextlib.ExitStack() as stack:
            align_to_weeks = stack.enter_context(
                unittest.mock.patch(
                    "cuteborg.scheduler.utils.align_to_weeks",
                )
            )

            result = utils.align_to_interval(
                self.dt,
                unittest.mock.sentinel.step,
                config.IntervalUnit.WEEK,
            )

        align_to_weeks.assert_called_once_with(
            self.dt,
            unittest.mock.sentinel.step,
        )

        self.assertEqual(
            result,
            align_to_weeks(),
        )

    def test_dispatch_to_month_impl(self):
        with contextlib.ExitStack() as stack:
            align_to_months = stack.enter_context(
                unittest.mock.patch(
                    "cuteborg.scheduler.utils.align_to_months",
                )
            )

            result = utils.align_to_interval(
                self.dt,
                unittest.mock.sentinel.step,
                config.IntervalUnit.MONTH,
            )

        align_to_months.assert_called_once_with(
            self.dt,
            unittest.mock.sentinel.step,
        )

        self.assertEqual(
            result,
            align_to_months(),
        )

    def test_dispatch_to_year_impl(self):
        with contextlib.ExitStack() as stack:
            align_to_years = stack.enter_context(
                unittest.mock.patch(
                    "cuteborg.scheduler.utils.align_to_years",
                )
            )

            result = utils.align_to_interval(
                self.dt,
                unittest.mock.sentinel.step,
                config.IntervalUnit.YEAR,
            )

        align_to_years.assert_called_once_with(
            self.dt,
            unittest.mock.sentinel.step,
        )

        self.assertEqual(
            result,
            align_to_years(),
        )


class Teststep_interval(unittest.TestCase):
    def setUp(self):
        self.dt = datetime(2016, 12, 2, 9, 17, 26, 432304)

    def test_reject_timezoned_timestamp(self):
        tz = pytz.timezone("Europe/Berlin")
        dt = tz.normalize(datetime.utcnow().replace(tzinfo=pytz.UTC))
        with self.assertRaisesRegex(
                ValueError,
                "needs naive datetime"):
            utils.step_interval(
                dt,
                unittest.mock.sentinel.step,
                unittest.mock.sentinel.unit
            )

    def test_minute_unit(self):
        self.assertEqual(
            utils.step_interval(
                self.dt,
                7,
                config.IntervalUnit.MINUTE,
            ),
            self.dt + timedelta(seconds=60*7),
        )

    def test_hour_unit(self):
        self.assertEqual(
            utils.step_interval(
                self.dt,
                7,
                config.IntervalUnit.HOUR,
            ),
            self.dt + timedelta(seconds=60*60*7),
        )

    def test_day_unit(self):
        self.assertEqual(
            utils.step_interval(
                self.dt,
                3,
                config.IntervalUnit.DAY,
            ),
            self.dt + timedelta(days=3),
        )

    def test_week_unit(self):
        self.assertEqual(
            utils.step_interval(
                self.dt,
                3,
                config.IntervalUnit.WEEK,
            ),
            self.dt + timedelta(days=7*3),
        )

    def test_month_unit(self):
        self.assertEqual(
            utils.step_interval(
                self.dt,
                3,
                config.IntervalUnit.MONTH,
            ),
            datetime(2017, 3, 2, 9, 17, 26, 432304)
        )

    def test_year_unit(self):
        self.assertEqual(
            utils.step_interval(
                self.dt,
                3,
                config.IntervalUnit.YEAR,
            ),
            datetime(2019, 12, 2, 9, 17, 26, 432304)
        )

    def test_raise_on_unknown_interval_unit(self):
        with self.assertRaisesRegex(
                ValueError,
                "invalid interval_unit"):
            utils.step_interval(
                self.dt,
                3,
                unittest.mock.sentinel.foo,
            )
