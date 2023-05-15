#!/usr/bin/env python3
"""reducer.py"""

import sys

user_id_utility = {}

# input comes from STDIN (note: this is the output from the mapper!)
for line in sys.stdin:

	# removing leading/trailing whitespaces
    line = line.strip()
	
	# parse the input elements    
    user_id, utility = line.split("\t")  

    # convert utility (currently a string) to float
    try:
        utility = float(utility)
    except ValueError:
        # if utility was not a number ignore/discard this line
        continue
    
    
    if user_id not in user_id_utility.keys():
        user_id_utility[user_id] = (0,0)
    
    utility_sum = user_id_utility[user_id][0] + utility
    utility_count = user_id_utility[user_id][1] + 1
    user_id_utility[user_id] = (utility_sum, utility_count)
    
for k,v in user_id_utility.items():
    print("%s\t%f" % (k, v[0]/v[1]))
    
 
