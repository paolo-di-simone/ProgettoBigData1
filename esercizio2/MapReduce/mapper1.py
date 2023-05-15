#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN (standard input)
for line in sys.stdin:

    # removing leading/trailing whitespaces
    line = line.strip()

    # split csv line over comma (text and summary not contains comma)
    fields = line.split(",")

    # compute utility
    if len(fields) == 10:

        user_id = fields[2]
        helpfulness_numerator = fields[4]
        helpfulness_denominator = fields[5]

        try:
            helpfulness_numerator = int(helpfulness_numerator)
            helpfulness_denominator = int(helpfulness_denominator)
        except ValueError:
			# if something was not a number ignore/discard this line
            print("%s\t%f" % (user_id, 0))
            continue

        if helpfulness_denominator != 0:
            utility = helpfulness_numerator / helpfulness_denominator
        else:
            # division by zero
            print("%s\t%f" % (user_id, 0))
            continue

    else:
        # row error format
        continue

    print("%s\t%f" % (user_id, utility))
