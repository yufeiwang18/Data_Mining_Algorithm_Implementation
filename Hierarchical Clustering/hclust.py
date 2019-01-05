# this file can be run under python2 within 10 seconds
from itertools import combinations
from scipy.spatial import distance
import pandas as pd
import numpy as np
import sys
from heapq import heappush,heappop,heapify,merge



# euclidean distance between points
def distancebp(p1,p2):
    #return float("{0:.3f}".format(distance.euclidean(p1,p2)))
    return distance.euclidean(p1,p2)

# calculate the centroid of a group of points
def midpoint(dict1,points):
    points=list(eval(str(points).replace("(","").replace(")","").replace(",","").replace(" ",", ")))
    centroid=np.array((0,0,0,0))
    for index in points:
        centroid=centroid+np.array(dict1.get(index))
    centroid=centroid/len(points)
    return centroid

# cluster points based on given cluster
def getlabels(list1):
    label=[i[2] for i in list1]
    index=[i[0] for i in list1]
    labels=pd.DataFrame({"index":index,"label":label})
    ls={}
    for i in set(labels["label"]):
        ls[str(i)]=list(labels.loc[labels["label"]==str(i),"index"])
    return ls

# form the first heap
def buildfheap(list1):
    list1=[(i[0],i[1]) for i in list1]
    newlist=list(combinations(list1,2))
    heap=[(distancebp(a[0][1],a[1][1]),(a[0][0],a[1][0]),midpoint(dict1,(a[0][0],a[1][0]))) for a in newlist]
    heapify(heap)
    #heap.sort()
    return heap

# remove clustered points and add new cluster
def updatere(results,tu):
    for t in tu:
        results.remove(t)
    results.append(tu)
    return results

# merge clusters
def heaploop(list1,heap):
    results=range(len(list1))
    clustered=[]
    npoint=0
    numofpoints=len(results)
    while npoint<(numofpoints-k):
        items=heap[0:3]
        item=heappop(heap)
        if len(clustered)==len(set(clustered)-set(item[1])): 
            npoint+=1
            clustered=clustered+list(item[1]) 
            results=updatere(results,item[1])
            newitems=[(distancebp(item[2],a[1]),
                       (item[1],a[0]),
                       midpoint(dict1,(a[0],item[1]))) 
                      for a in list1 
                      if(a[0] not in clustered) ]
            list1=list1+[(item[1],item[2])]
            heap=list(merge(heap,newitems))
            heapify(heap)

    return results

# print final k clusters
def getclus(results):
    t=1
    cluresult=[]
    print(str(results))
    for i in results:
        if type(i)==int:
            clu=[i]
            cluresult.append(clu)
        else: 
            clu=list(eval(str(i).replace("(","").replace(")","").replace(",","").replace(" ",", ")))
            clu.sort()
            cluresult.append(clu)
        print("Cluster"+str(t)+": "+str(clu))
        t+=1
    return cluresult

# evaluate cluster result
def evaluation(cluresult,labels):
    pairs=[]
    for i in cluresult:
        pairs=pairs+list(combinations(i,2))
    realpairs=[]
    for i in labels:
        realpairs=realpairs+list(combinations(labels.get(i),2))
    truepairs=set(pairs)-(set(pairs)-set(realpairs))
    precision=float(len(truepairs))/float(len(pairs))
    recall=float(len(truepairs))/float(len(realpairs))
    return precision,recall


if __name__ == "__main__":
    try:
        list1=[]
        dict1={}
        j=0
        inputdata=sys.argv[1]
        k=int(sys.argv[2])
        with open(inputdata) as f:
            for i in f:
                dict1[j]=(eval(i[:15]))
                list1.append((j,(eval(i[:15])),i[16:].replace("\n","")))# :15 16:
                j+=1

        labels=getlabels(list1)
        heap=buildfheap(list1)
        results=heaploop(list1,heap)
        cluresult=getclus(results)

        precision,recall=evaluation(cluresult,labels)
        print("Precision = "+str(precision)+", recall = "+str(recall))
    except:
        print("If you are using python3, there might be some error, but this can run in python2 within 10 seconds. :)")

