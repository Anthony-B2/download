#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split, array, array_min, concat, least, collect_set, size, sum

# This function will apply RDD transformation before CCF iterations
# It returns graph Dataframe to use for CCF function
def prepare_dataset(dataset):
    graph = dataset.filter(lambda x: "#" not in x)\
               .map(lambda x : x.split("\t"))\
               .map(lambda x : (int(x[0]), int(x[1])))
    
    graph = spark.createDataFrame(graph).selectExpr("_1 as key", "_2 as value")

    return graph

# This function will perform CCF Iteration
# It returns the graph with new pairs calculated by CCF iterations
def Calculate_CCF(graph):
    iteration = 0
    done = False

    while not done:

        iteration += 1
        startPair = newPair.value

        # CCF-Iterate MAP
        ccf_iterate_map = graph.union(graph.select(col("value").alias("key"), col("key").alias("value")))

        # CCF-Iterate REDUCE
        ccf_iterate_reduce_pair = ccf_iterate_map.groupBy(col("key")).agg(collect_set("value").alias("value"))\
                                            .withColumn("min", least(col("key"), array_min("value")))\
                                            .filter((col('key')!=col('min')))

        newPair += ccf_iterate_reduce_pair.withColumn("count", size("value")-1).select(sum("count")).collect()[0][0]

        ccf_iterate_reduce = ccf_iterate_reduce_pair.select(col("min").alias("a_min"), concat(array(col("key")), col("value")).alias("valueList"))\
                                                    .withColumn("valueList", explode("valueList"))\
                                                    .filter((col('a_min')!=col('valueList')))\
                                                    .select(col('a_min').alias("key"), col('valueList').alias("value"))

        # CFF-Dedup MAP & REDUCE
        ccf_dedup_reduce = ccf_iterate_reduce.distinct()

        graph = ccf_dedup_reduce

        if startPair == newPair.value:
        done = True

        print("Itération : ", iteration, "Number of newPair : ", newPair.value)
    
    return graph

# MAIN #  
if __name__ == "__main__":

    sc = pyspark.SparkContext(appName="Spark_RDD")
    spark = SparkSession.builder.getOrCreate()
    newPair = sc.accumulator(0)
    
    dataset_path = "/user/user335/dataset/ccf"
    dataset = sc.textFile(dataset_path + "/web-Google.txt", use_unicode="False")

    graph = prepare_dataset(dataset)

    t1 = time.perf_counter()
    graph = Calculate_CCF(graph)
    t2 = time.perf_counter()

    print("calculation time (s) :", t2 - t1)