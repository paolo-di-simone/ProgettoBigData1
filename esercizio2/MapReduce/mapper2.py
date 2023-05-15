#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN (standard input)
for line in sys.stdin:

    # removing leading/trailing whitespaces
    line = line.strip()
	
	# parse the input elements    
    user_id, mean_utility = line.split("\t")  

    # convert utility (currently a string) to float
    try:
        mean_utility = float(mean_utility)
    except ValueError:
        # if utility was not a number ignore/discard this line
        continue
        
    print("%s\t%f" % (user_id, mean_utility))
