#!/usr/bin/env python3
import sys
import logging
from collections import Counter

logging.basicConfig(level=logging.INFO)
map = {}
word = {}

def word_order(words):
    # Count the frequency of each word
    word_counts = Counter(words)
    # Sort the words based on frequency in descending order
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True) # l= [ "ciao", "ciao", "cane", "prova", "prova", "prova"]
    return sorted_words # l= [(prova, 3), (ciao, 2), (cane,1)]

#----------------------------------------------------------------------------------------#
for line in sys.stdin: 
    
    line = line.strip()
    #if(len(line.split("/t")) ==2):
    year, id_text = line.split("\t")
       # if(len(id_text.split("~")) == 2):
            #logging.info("-------------------IL CAMPO LINE E'----------------------------- ", line)


    
    
            #logging.info("-------------------IL CAMPO LINE E'----------------------------- ", id_text)

    id, text= id_text.split("~") #[0] #id_prodotto
        #text = id_text.split("~")[1] #testo
        #logging.info("-------------------IL TESTO E'----------------------------- ", text)
        
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

#logging.info("stampa ", map)
#-----------------------------------------------------------------------------------------------#

ordered_count_id ={}  #{anno:{id:number, id:number}, anno:{id:number, id:number}} ordina ogni sottodizionario per number
for k,v in map.items():
    sorted_word_count = sorted(v.items(),
                           key=lambda item: item[1],
                           reverse=True)
    
    ordered_dict = {k: v for k, v in sorted_word_count}
    ordered_count_id[k] = ordered_dict


#logging.info("stampa map ordinato", ordered_count_id)
    

for year, id in ordered_count_id.items(): #{anno:{id:number, id:number}, anno:{id:number, id:number}}
       t=0
       for id_2,count in id.items(): 
           word_list = word_order(word[year][id_2])
           logging.info("stampa parole ordinato", word_list)
           print("%s\t%s\t%i\t%s" % (year,id_2,count,str(word_list[:5])))
           t+=1
           if (t == 10):
               break
   

#order = { "2000" :{ "2" :10,
 #                   "8" :20,
 #                   "4": 45
     
 #                       },

 #          "1999" : { "1": 15,
 #                     "9": 3,
  #                    "4":8
               
  #                  }            
  #     }

#word_list = { "2000" :{ "2" :["prova", "ciao", "prova" , "ciao", "gatto", "topo", "topo", "gatto", "cane","cane", "cane", "ao" ],
#                    "8" :["prova", "ciao", "prova" , "ciao", "gatto", "topo", "topo", "gatto", "cane","cane", "cane", "ao" ],
#                    "4": ["prova", "ciao", "prova" , "ciao", "gatto", "topo", "topo", "gatto", "cane","cane", "cane", "ao" ]
     
 #                       },

  #         "1999" : { "1": ["prova", "ciao", "prova" , "ciao", "gatto", "topo", "topo", "gatto", "cane","cane", "cane", "ao" ],
  #                    "9": ["prova", "ciao", "prova" , "ciao", "gatto", "topo", "topo", "gatto", "cane","cane", "cane", "ao" ],
  #                 "4":["prova", "ciao", "prova" , "ciao", "gatto", "topo", "topo", "gatto", "cane","cane", "cane", "ao" ]
               
  #                  }            
  #     }





   
   # i=0
    #for k in sorted_dict:
     #   year_id = k
     #   count = sorted_dict[year_id]
     #   print("%s\t%i" % (year_id, count))
      #  i+=1 
      #  if (i==10):
       #     break



     
#count_word = {}
#for k,v in ordered_count_id.items():
#    i=0
 #   for m,n in v.items(): 
#        year_id = str(k) +"-"+str(m)
#        year = k
#        id = v
#        if(id_word[year_id] != []):
#            for elem in id_word[year_id]:
#                if year not in count_word:
#                    count_word[year] = {}
#                if elem not in count_word[year]:
#                    count_word[year][id]= []
#                
#                count_word[year][id].append(elem)

        #print("%s\t%s\t%s" % (k,m,n))
 #       i+=1 
 #       if (i==10):
 #           break

#ordered_count_words ={} 
 #for k,v in count_word.items():
 #   sorted_word_count = sorted(v.items(),
 #                          key=lambda item: item[1],
 #                          reverse=True)
    
 #   ordered_dict = {k: v for k, v in sorted_word_count}
 #   ordered_count_words[k] = ordered_dict


#for k,v in ordered_count_words.items():
#    t  = 0
#    for i,j in v.items():
        #print("%s\t%i" % (i,j))
#        t+=1 
#        if (t==5):
 #           break
     


 #(year,text)-> dict [id,list[parole]] -> parola = split(text, spazio) -> (year-text, id) -> 