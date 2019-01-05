from pyspark import SparkContext
from itertools import combinations
import sys
t=0.2

### find candidate pairs from bands
# x is row number, i is range from 0 to 19
def h(x,i):
    return (3*x+13*i)%100

def candidates(lines):
    li=[]
    for j in range(0,20):
        li+=lines.map(lambda x:x.split(",",1)).flatMapValues(
            lambda s:s.split(",")).map(
            lambda x:((x[0],int(j/4)),h(eval(x[1]),j))).reduceByKey(min).collect()

    data = sc.parallelize(li, 4)
    li=data.groupByKey().map(lambda x:(x[0],list(x[1]))).map(lambda x:((x[0][1],str(x[1])),x[0][0])).groupByKey().map(lambda x:(x[0],list(x[1]),len(list(x[1])))).filter(lambda x:x[2]>1).collect()

    candidate=[]
    for i in li:
        test=[int(j[1:]) for j in i[1]]
        test.sort()
        test=["U"+str(j) for j in test]
        candidate+=list(combinations(test,2))
    candidate=list(set(candidate))
    return candidate

### find candidate similarity on whole dataset
def recheck(lines,candidate):
    data={}
    recommend={}
    da=lines.map(lambda x:x.split(",",1)).map(lambda x:(x[0],eval(x[1]))).collect()
    for i in da:
        data[i[0]]=i[1]
        recommend[i[0]]=[]
    for j in candidate:
        #print(data.get(j[0]))
        if(type(data.get(j[0]))==int):
            rdd1 = sc.parallelize([data.get(j[0])])
        else:
            rdd1 = sc.parallelize(data.get(j[0]))
        if(type(data.get(j[1]))==int):
            rdd2 = sc.parallelize([data.get(j[1])])
        else:
            rdd2 = sc.parallelize(data.get(j[1]))
        intersection=len(rdd1.intersection(rdd2).collect())
        jaccard=(intersection*2)/(len(rdd1.collect())+len(rdd2.collect()))
        if(jaccard>t):
            recommend[j[0]].append(((round(1-jaccard,2),int(j[1][1:]),data.get(j[1]))))
            recommend[j[1]].append(((round(1-jaccard,2),int(j[0][1:]),data.get(j[0]))))
    return recommend

### make recommendation
def rec(recommend):
    re=[]
    for i in recommend:
        te=recommend.get(i)
        te.sort(reverse=False)
        removie=[]
        for j in range(len(te)):
            if(j<5):
                if(type(te[j][2])==int):
                    removie.append(((i,te[j][2]),1))
                else:
                    removie+=[((i,p),1) for p in te[j][2]]
        re+=removie
    return re

#get first 3 movies
def sort(x):
    test=x[1]
    test.sort()
    test=[test[p][1] for p in range(len(test)) if p<3]
    return (int(x[0][1:]),test)


def main(lines):
    totalnum=len(lines.collect())
    candidate=candidates(lines)
    recommend=recheck(lines,candidate)
    re=rec(recommend)
    rdd=sc.parallelize(re,3)
    output=rdd.groupByKey().map(
        lambda x:(x[0],sum(x[1]))).map(
        lambda x:(x[0][0],(int(totalnum-x[1]),x[0][1]))).groupByKey().map(
        lambda x:(x[0],list(x[1]))).map(
        lambda x: sort(x)).sortByKey(True).map(lambda x:("U"+str(x[0]),x[1])).collect()

    output=str(output).replace("(","").replace("), ","\n").replace("[","").replace("]","").replace("\'","").replace(")","").replace(" ","")
    return output

if __name__ == "__main__":
    input=sys.argv
    inputpath=input[1]
    outputpath=input[2]

    sc = SparkContext.getOrCreate()
    lines = sc.textFile(inputpath)
    
    output=main(lines)
    text_file = open(outputpath, "w")
    text_file.write(output)
    text_file.close()