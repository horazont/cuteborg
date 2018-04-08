########################################################################
# File name: utils.py
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
def print_table(header, rows):
    col_max_widths = [len(hdr) for hdr in header]

    for row in rows:
        for col, cell in enumerate(row):
            col_max_widths[col] = max(
                len(cell),
                col_max_widths[col],
            )

    fmt = " | ".join(
        "{{:<{w}s}}".format(
            w=width
        )
        for width in col_max_widths,
    )

    print(fmt.format(*("="*w for w in col_max_widths)))
    print(fmt.format(*header))
    print(fmt.format(*("-"*w for w in col_max_widths)))

    for row in rows:
        print(fmt.format(*row))
