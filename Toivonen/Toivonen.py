#from numpy import random
import pandas as pd
import time
import random
from itertools import combinations
import sys

# read data from file
input=sys.argv
f = open(input[1],'r')
message = f.read()
f.close()
data=message.split("\n")
samplelength=int(0.1*len(data))
t_ratio=4/15
t=t_ratio*samplelength
t_total=t_ratio*len(data)
data2=[]
for i in range(len(data)):
    j=eval(data[i])
    if (type(j)==int):
        te=[j]
    else:
        te=list(set(j))
        te.sort()
    data2.append(te)

    start_time = time.time()


# functions
# give list ll and treshold tre, return values whose count is >= tre
def count(l1,tre):
    va=[1]*len(l1)
    dd=pd.DataFrame({"key":l1,"value":va})
    dd=pd.DataFrame(dd.groupby("key")["value"].sum())
    temp=list(dd[dd["value"]>=tre].index)
    return temp


# get all frequent itemset from sampledata
def findallf(sampledata):
    ppairs={}
    for i in sampledata:
        for j in range(0,len(i)):
            # count for 1-item
            key1=i[j]
            if key1 in ppairs.keys():  
                ppairs.update({key1:(ppairs.get(key1)+1)})   
            else:
                ppairs.update({key1:1})   
            for p in range(j+1, len(i)):
                # count for 2-items
                key2=(i[j],i[p])
                if key2 in ppairs.keys():
                    ppairs.update({key2:(ppairs.get(key2)+1)})
                else:
                    ppairs.update({key2:1})
                for q in range(p+1,len(i)):
                    key3=(i[j],i[p],i[q])
                    if key3 in ppairs.keys():
                        ppairs.update({key3:(ppairs.get(key3)+1)})
                    else:
                        ppairs.update({key3:1})
    ppairs=pd.DataFrame({"key":list(ppairs.keys()),"value":list(ppairs.values())})
    ppairs=ppairs[ppairs["value"]>=t]

    return ppairs

# based on frequent-1 generate frequent2 candidates
def findf2c(frequent1):
    frequent2=[]
    f1=list(frequent1)
    f1.sort()
    for i in range(len(f1)):
        for j in range(i+1,len(f1)):
            frequent2.append((f1[i],f1[j]))
    return frequent2

# generate 3-elements itemset based on frequent 2-element itemset
def findf3c(frequent2):
    frequent3={}
    # at least three 2-element itemsets can generate 1 3-elements itemset, so at least there are three unique values and
    # every one occurs at leaset 2 times
    b=3
    uniquelen=len(set([j for i in frequent2 for j in i ]))
    orilen=len([j for i in frequent2 for j in i ])

    # generate 3-items candidates
    if (len(frequent2)>=3)&(uniquelen<=(orilen-b)):
        f2=[j for i in frequent2 for j in i ]
        base=count(f2,2)
        base.sort()
        ll=len(base)
        for a in range(ll):
            for b in range(a+1,ll):
                key1=(base[a],base[b])
                if(key1 in frequent2):
                    for c in range(b+1,ll):
                        key2=(base[a],base[c])
                        if (key2 in frequent2):
                            key3=(base[b],base[c])
                            if(key3 in frequent2):
                                # generate a 3-elements candidate
                                key=(base[a],base[b],base[c])
                                frequent3.update({key:0})
    return frequent3

def findfn(uniquenb,negativeborder):
    nb=dict((el,0) for el in negativeborder)
    for i in data2:
        base=set(i)
        base=list(uniquenb-(uniquenb-base))
        base.sort()
        for i in range(len(base)):
            key1=base[i]
            if(key1 in nb.keys()):
                nb.update({key1:(nb.get(key1)+1)})
            for j in range(i+1,len(base)):
                key2=(base[i],base[j])
                if(key2 in nb.keys()):
                    nb.update({key2:(nb.get(key2)+1)})
                for n in range(j+1,len(base)):
                    key3=(base[i],base[j],base[n])
                    if (key3 in nb.keys()):
                        nb.update({key3:(nb.get(key3)+1)})

    #false negative
    fn=pd.DataFrame({"keys":list(nb.keys()),"values":list(nb.values())})
    t=(4/15)*300
    fnlist=fn[fn["values"]>=t]
    return list(fnlist["keys"])

def findrealf(ppairs):
    frequent1=[ppairs.loc[i,"key"] for i in ppairs.index if type(ppairs.loc[i,"key"])==int]
    samplef=ppairs["key"]
    rf={}
    num=0
    inum=-1

    for i in data2:
        inum+=1
        base=set(i)-(set(i)-set(frequent1))
        base=list(base)
        base.sort()
        f=tuple(base)+tuple([k for k in combinations(base,2)])+tuple([k for k in combinations(base,3)])+tuple([k for k in combinations(base,4)])
        bf=set(f)-(set(f)-set(samplef))
        for j in bf:
            if(j==(356, 588)):
                num+=1
            if (j in rf):
                rf[j]+=1
            else:
                rf[j]=1
    realf=[i for i in rf.keys() if rf.get(i)>=t_total]
    return realf

def findf4c(frequent3):
    frequent3.sort()
    f4=combinations(frequent3,4)
    ff4=[]
    for i in f4:
        li=()
        for j in i:
            li+=j
        if(len(set(li))==4):
            ff4.append(tuple(set(li)))
    return ff4

sampleindexli=[36, 259, 64, 63, 168, 214, 87, 61, 125, 154, 122, 128, 70, 28, 261, 97, 141, 49, 183, 245, 44, 259, 250, 193, 102]
start_time = time.time()

# main
for seed in range(1,270):
    loop_time = time.time()
    file = open("OutputForIteration_"+str(seed)+".txt","w")
    file.write("Sample Created:\n")
    random.seed(seed)
    sampleindex=random.randint(0,270)
    sampledata=[]
    sampleindex=sampleindexli[seed-1]
    for i in range(sampleindex,sampleindex+30):
        j=eval(data[i])
        if (type(j)==int):
            te=[j]
        else:
            te=list(set(j))
            te.sort()
        sampledata.append(te)
    file.write(str(sampledata)+"\n")
    sd=[]
    for i in sampledata:
        sd=sd+i
    # get all frequent itemset from sampledata
    ppairs=findallf(sampledata)
    # find frequent 1 item
    frequent1=[ppairs.loc[i,"key"] for i in ppairs.index if type(ppairs.loc[i,"key"])==int]
    # find frequent 2 item-set
    frequent2=[ppairs.loc[i,"key"] for i in ppairs.index if(type(ppairs.loc[i,"key"])==tuple) if(len(ppairs.loc[i,"key"])==2) ]
    

    # check frequent itemset by whole dataset
    realf=findrealf(ppairs)
    f1=[i for i in realf if type(i)==int]
    f1.sort()
    f2=[i for i in realf if type(i)==tuple if (len(i)==2)]
    f2.sort()
    f3=[i for i in realf if type(i)==tuple if (len(i)==3)]
    file.write("frequent itemsets:\n")
    file.write("frequent1: "+str(f1)+"\n")
    file.write("frequent2: "+str(f2)+"\n")
    file.write("frequent3: "+str(f3)+"\n")
    # find negative border
    # firstly, get all frequent itemset candidate
    frequent2=findf2c(frequent1)
    frequent3=findf3c(frequent2)
    scandidate=list(set(sd))+list(frequent3.keys())+frequent2#+frequent4

    # secondly, find those itemset who only in candidate list but not in frequent list
    negativeborder=set(scandidate)-set(ppairs["key"])
    file.write("\nnegative border:\n")
    nn=list(negativeborder)
    nn1=[i for i in nn if(type(i)==int)]
    nn2=[i for i in nn if(type(i)==tuple) if(len(i)==2)]
    nn3=[i for i in nn if(type(i)==tuple) if(len(i)==3)]
    nn1.sort()
    nn2.sort()
    nn3.sort()
    file.write(str([nn1,nn2,nn3]))
    # find false negative
    uniquenb=repr(negativeborder).replace("(","").replace(")","")
    uniquenb=set(eval(uniquenb))
    fn=findfn(uniquenb,negativeborder)
    file.write("\nfalse negative:\n")
    file.write(str(fn))
    file.close()
    print("seed: ",seed,"\n sampleindex:",sampleindex,"\n false negative: ",fn)
    print("--- %s seconds ---" % (time.time() - loop_time))
    if(len(fn)==0):
        break

print("--- %s seconds ---" % (time.time() - start_time))
print("\n")
