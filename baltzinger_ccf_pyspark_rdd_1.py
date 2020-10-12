#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import pyspark

# This function will apply RDD transformation before CCF iterations
# It returns graph RDD to use for CCF function
def prepare_dataset(dataset):
    graph = dataset.filter(lambda x: "#" not in x)\
                .map(lambda x : x.split("\t"))\
                .map(lambda x : (int(x[0]), [int(x[1])]))
    return graph

# countNewPair function to know if additional CCF iteration is needed
def countNewPair(x):
  global newPair
  start = 0
  for value in x[1]:
    if value != x[2]:
      start = start + 1
  newPair += start

# This function will perform CCF Iteration
# It returns the graph with new pairs calculated by CCF iterations
def Calculate_CCF(graph):
    iteration = 0
    done = False
    
    while not done:

        iteration += 1
        startPair = newPair.value
        
        # CCF-Iterate MAP
        ccf_iterate_map = graph.union(graph.map(lambda x : (x[1][0], [x[0]])))

        # CCF-Iterate REDUCE
        ccf_iterate_reduce_pair = ccf_iterate_map.reduceByKey(lambda x,y : x+y)\
                        .map(lambda x : (x[0], x[1], min(x[0], min(x[1]))))\
                        .filter(lambda x: x[0] != x[2])
        ccf_iterate_reduce_pair.foreach(countNewPair)
        ccf_iterate_reduce = ccf_iterate_reduce_pair.map(lambda x : (x[2], x[1] + [x[0]]))\
                        .flatMapValues(lambda x : x)\
                        .filter(lambda x: x[0] != x[1])\
                        .map(lambda x : (x[0], [x[1]]))

        # CCF-Dedup - MAP
        ccf_dedup_map = ccf_iterate_reduce.map(lambda x : (((x[0], x[1][0]),None)))
        
        # CCF-Dedup - REDUCE
        ccf_dedup_reduce = ccf_dedup_map.groupByKey().map(lambda x : (x[0][0], [x[0][1]]))
        
        graph = ccf_dedup_reduce

        if startPair == newPair.value:
            done = True

        print("Itération : ", iteration, "Number of newPair : ", newPair.value)
    
    return graph

# MAIN #  
if __name__ == "__main__":

    sc = pyspark.SparkContext(appName="Spark_RDD")
    
    newPair = sc.accumulator(0)
    
    dataset_path = "/user/user335/dataset/ccf"
    dataset = sc.textFile(dataset_path + "/web-Google.txt", use_unicode="False")

    graph = prepare_dataset(dataset)

    t1 = time.perf_counter()
    graph = Calculate_CCF(graph)
    t2 = time.perf_counter()

    print("calculation time (s) :", t2 - t1)