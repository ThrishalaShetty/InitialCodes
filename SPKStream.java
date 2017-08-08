package com.university.learnspark.java;

import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;


public class WordCount {
  public static void main(String[] args) throws Exception {


SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

		
// Create a DStream that will connect to hostname:port

JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 0000);


// Split each line into words
JavaDStream<String> words = input.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterable<String> call(String x) {
          return Arrays.asList(x.split(" "));
        }});


// Count each word in each batch
JavaPairDStream<String, Integer> counts = words.mapToPair(
      new PairFunction<String, String, Integer>(){
        public Tuple2<String, Integer> call(String x){
          return new Tuple2(x, 1);
        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer x, Integer y){ return x + y;}});

// Print the first ten elements of each RDD generated in this DStream to the console
counts.print();
sc.start();              // Start the computation
sc.awaitTermination();    
	
  }
}
	
