#!/usr/bin/env python3
"""mapper.py"""

import sys
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

start_time = time.time()

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

end_time = time.time()
execution_time = end_time - start_time

logger.info("TEMPO mapper2: %s s", execution_time)
