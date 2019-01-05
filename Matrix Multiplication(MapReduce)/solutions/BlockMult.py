from pyspark import SparkContext
import pandas as pd
import numpy as np
import sys

b=3

#def mapper
def mapfuncA(x):
    aftermap=[]
    x=eval(x)
    rindex=x[0][0]
    value=x[1]
    for cindex in range(1,b+1):
        index=(rindex,cindex)
        values=("A",x[0][1],value)
        aftermap.append((index,values))
    return aftermap

def mapfuncB(x):
    aftermap=[]
    x=eval(x)
    cindex=x[0][1]
    value=x[1]
    for rindex in range(1,b+1):
        index=(rindex,cindex)
        values=("B",x[0][0],value)
        aftermap.append((index,values))
    return aftermap

#def reducer
def reducefunc(x):
    aid=[]
    avalue=[]
    arow=[]
    acolumn=[]
    bid=[]
    bvalue=[]
    brow=[]
    bcolumn=[]
    for i in x:
        if i[0]=="A":
            for j in i[2]:
                aid.append(i[1])
                arow.append(j[0])
                acolumn.append(j[1])
                avalue.append(j[2])
        else:
            for j in i[2]:
                bid.append(i[1])
                brow.append(j[0])
                bcolumn.append(j[1])
                bvalue.append(j[2])
    at=pd.DataFrame({"aindex":aid,"arow":arow,"acolumn":acolumn,"avalue":avalue})
    bt=pd.DataFrame({"bindex":bid,"brow":brow,"bcolumn":bcolumn,"bvalue":bvalue})
    t=at.merge(bt,left_on=["aindex","acolumn"],right_on=["bindex","brow"],how="inner")
    t["product"]=t["avalue"]*t["bvalue"]
    result=[]
    t=t[["arow","bcolumn","product"]]
    t=t.groupby(["arow","bcolumn"],as_index=False)["product"].sum()
    for i in range(len(t)):
        result.append(t.loc[i,:].tolist())
    return result


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    input=sys.argv
    Afilepath=input[1]
    Bfilepath=input[2]
    outputpath=input[3]

    MatrixA = sc.textFile(Afilepath)
    MatrixB = sc.textFile(Bfilepath)

    Alist=MatrixA.flatMap(lambda x: mapfuncA(x)).collect()
    Blist=MatrixB.flatMap(lambda x: mapfuncB(x)).collect()
    rdd=sc.parallelize(Alist+Blist)
    result=rdd.groupByKey().mapValues(reducefunc).sortByKey().filter(lambda x:len(x[1])>0).map(lambda x: str(x)).map(lambda x:x[1:-1]).collect()

    text_file = open(outputpath, "w")
    for i in result:
        text_file.write(i+"\n")
    text_file.close()
