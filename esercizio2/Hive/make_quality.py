#!/usr/bin/env python3
import sys

# read input from stdin
for line in sys.stdin:

    try:
        # remove whitespaces and trailing characters
        line = line.strip()

        # parse name and unix date using TAB as a separator
        user_id, helpfulness_numerator, helpfulness_denominator = line.split("\t")

        # try to convert the helpfulness_numerator and helpfulness_denominator to an integers
        try:
            helpfulness_numerator = float(helpfulness_numerator)
            helpfulness_denominator = float(helpfulness_denominator)
        except ValueError:
            print("%s\t%f" % (user_id, 0))
            continue

        if helpfulness_denominator != 0:
            quality = helpfulness_numerator / helpfulness_denominator
            # print output items to stdout, using TAB as a separator
            print("%s\t%f" % (user_id, quality))
        else:
            print("%s\t%f" % (user_id, 0))
            continue

    except:
        import sys
        print(sys.exc_info())
