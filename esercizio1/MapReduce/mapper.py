#!/usr/bin/env python3
import sys

for line in sys.stdin:

    line = line.strip()

    fields = line.split(",")

    try:
        year = int(fields[7])
        product_id = fields[1].strip()
        text = fields[9].strip()
        if text == "":
            value = product_id+"~-~"+"b"
        else:
            value = product_id+"~-~"+text
        print('%i\t%s' % (year, value))
    except:
        continue
