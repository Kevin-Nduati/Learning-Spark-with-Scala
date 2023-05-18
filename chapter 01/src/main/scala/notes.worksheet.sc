// DEFINITIONS

// * Application - a user programme built on spark using its APIs. it consists of a driver program and executors on the cluster
// * SparkSession - an object that provides a point of entry to interact with underlying spark functionality and allows programming spark with its apis
// * Job - a parallel computation consisting of multiple tasks that gets spawned in response to a spark action
// * Stage - each job gets divided into smaller sets of tasks called stages that depend on each other
// * Task - a single unit of work or execution that will be sent to a spark executor


// Transformations transform a spark dataframe into a new dataframe without altering the original data, giving it the property of immutability
// Actions trigger the lazy evaluation of all the recorded transformations. 
// while lazy evaluatopmn allows spark to optimize your queries by peeking into your chained transformations, lineage and data immutability provide 
// fault tolerance


import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("testingSpark").master("local").getOrCreate()

val strings = spark.read.text("../README.md")

val filtered = strings.filter(col("value").contains("Spark"))

filtered.count()



// NARROW AND WIDE TRANSFORMATIONS

// Any transformation where a single output partition can be computed from a single input partition is a narrow transformation. For example, filter
// and contains represent narrow transformations because they can operate on a single partition and produce the resulting output partition
//  without an exachange of data

// However, groupBy() or orderBy() instruct spark to perform wide transformations, where data is read in, combined and written to disk