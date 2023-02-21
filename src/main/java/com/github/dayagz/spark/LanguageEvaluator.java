package com.github.dayagz.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.streaming.StreamInputFormat;
import org.apache.hadoop.streaming.StreamXmlRecordReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.wikiclean.WikiClean;
import org.wikiclean.languages.English;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

@Slf4j
public class LanguageEvaluator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Word Counter");

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf);) {
            JobConf jobConf = new JobConf();
            jobConf.set("stream.recordreader.class", StreamXmlRecordReader.class.getName());
            jobConf.set("stream.recordreader.begin", "<page>");
            jobConf.set("stream.recordreader.end", "</page>");
            FileInputFormat.addInputPaths(jobConf, "WikiPages_BigData.xml");
            JavaPairRDD<Text, Text> wikiDocs = sc.hadoopRDD(jobConf, StreamInputFormat.class, Text.class, Text.class);

            JavaRDD<String> wikiContent = wikiDocs.mapPartitions((FlatMapFunction<Iterator<Tuple2<Text, Text>>, String>) tuple2Iterator -> {
                WikiClean cleaner = new WikiClean.Builder()
                        .withLanguage(new English())
                        .withTitle(false)
                        .withFooter(false).build();
                return StreamSupport.stream(Spliterators.spliteratorUnknownSize(tuple2Iterator, Spliterator.ORDERED), false)
                        .map(x -> x._1().toString())
                        .map(cleaner::clean).iterator();
            });
            JavaRDD<String> tokenizedWikiData = wikiContent.flatMap((FlatMapFunction<String, String>) text -> Arrays.stream(text.split("\\W+")).iterator());
            JavaRDD<String> pertinentWikiData = tokenizedWikiData.filter(wikiToken -> wikiToken.length() > 3);

            JavaPairRDD<Object, String> distinctWordsSortedByLength = pertinentWikiData.distinct()
                    .sortBy(String::length, false, 4)
                    .sample(false, .1).keyBy(String::length);

            distinctWordsSortedByLength.collect().forEach(tuple -> {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                    log.info(String.valueOf(tuple));

            });



        }
    }
}
