#!/usr/bin/env python3
import sys
import datetime
import logging

#The keys are divided among all the Reduce tasks, so all key-value pairs with the same key wind up
#at the same Reduce task.
# Programmers specify two functions:
#map (k1, v1) → [(k2, v2)]
#reduce (k2, [v2]) → [(k3, v3)]
# All values with the same key are reduced together

logging.basicConfig(level=logging.INFO)


i= 0
for line in sys.stdin:
  fields = line.split(",")
  #try:
  #  dt = datetime.datetime.utcfromtimestamp(int(fields[7])).strftime('%Y-%m-%dT%H:%M:%SZ')
  #except ValueError:
  #  continue

  if (fields[1] == ""):
    continue
  if (fields[9] == ""):
    value  = str(fields[1])+"~"+"b"
  
  if(fields[9] != ""):
    value  = str(fields[1])+"~"+str(fields[9])
  #if (i<=12):
    #logging.info("-------------------IL CAMPO text è----------------------------- ", value)
  #i+=1
  print('%s\t%s' % ( str(fields[7]), value)) #anno , productId-testo