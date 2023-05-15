#!/usr/bin/env python3
"""reducer.py"""

import sys

user_id_mean_utility = {}

# input comes from STDIN (note: this is the output from the mapper!)
for line in sys.stdin:

	# removing leading/trailing whitespaces
    line = line.strip()
	
	# parse the input elements    
    user_id, mean_utility = line.split("\t")  

    # convert mean_utility (currently a string) to float
    try:
        mean_utility = float(mean_utility)
    except ValueError:
        # if mean_utility was not a number ignore/discard this line
        continue
    
    user_id_mean_utility[user_id] = mean_utility


user_id_mean_utility = dict(sorted(user_id_mean_utility.items(), key=lambda item: item[1], reverse=True))

for k,v in user_id_mean_utility.items():
    print("%s\t%f" % (k, v))
    
    
 
