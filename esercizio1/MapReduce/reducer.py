#!/usr/bin/env python3
import sys
from collections import Counter

map = {}
word = {}

def word_order(words):
    # Count the frequency of each word
    word_counts = Counter(words)
    # Sort the words based on frequency in descending order
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True) # l= [ "ciao", "ciao", "cane", "prova", "prova", "prova"]
    return sorted_words # l= [(prova, 3), (ciao, 2), (cane,1)]

for line in sys.stdin:

    line = line.strip()
    year, id_text = line.split("\t")

    id, text= id_text.split("~-~") #[0] #id_prodotto

    text = text.split(" ") #lista parole

    if year not in word:
        word[year] = {}
    if id not in word[year]:
        word[year][id] = []   # {anno:{id:[],}

    for elem in text:  # per tutti gli elemnti nella lista text
        if(len(elem) >= 4):
            word[year][id].append(elem) # {anno:{id:[ parola , ....], id:[ parola , ....]}, anno:{id:[ parola , ....], id:[ parola , ....]}}


    if year not in map:
        map[year] = {}
    if id not in map[year]:
        map[year][id] = 0 #{anno:{id:number, id:number}, anno:{id:number, id:number}}
    map[year][id]+=1

ordered_count_id ={}  #{anno:{id:number, id:number}, anno:{id:number, id:number}} ordina ogni sottodizionario per number
for k,v in map.items():
    sorted_word_count = sorted(v.items(),
                           key=lambda item: item[1],
                           reverse=True)

    ordered_dict = {k: v for k, v in sorted_word_count}
    ordered_count_id[k] = ordered_dict


for year, id in ordered_count_id.items(): #{anno:{id:number, id:number}, anno:{id:number, id:number}}
       t=0
       for id_2,count in id.items():
           word_list = word_order(word[year][id_2])
           print("%s\t%s\t%i\t%s" % (year,id_2,count,str(word_list[:5])))
           t+=1
           if (t == 10):
               break
