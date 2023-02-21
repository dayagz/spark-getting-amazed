package com.github.dayagz.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;

@Slf4j
public class WordCounter {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Word Counter");

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf);) {
            JavaRDD<String> textFile = sc.textFile("input.txt");
            //get the words
            JavaRDD<String> wordsFromFile = textFile.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split("\\W+")).iterator());
            //Assign count to each word, default 1
            JavaPairRDD<String, Integer> countPrep = wordsFromFile.mapToPair(word -> new Tuple2<>(word, 1));
            //Count the number of occurrences of a word and sort
            JavaPairRDD<String, Integer> countData = countPrep.reduceByKey(Integer::sum).sortByKey();
            //Collect the output and print
            countData.collect().forEach(tuple2 -> log.info(String.valueOf(tuple2)));

        }


    }
}
