package com.fanxan.Memory.resources;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Immutable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * Project Memory
 * User: Fannan
 * Date: 3/31/2024
 * Time: 10:54 AM
 * To change this template use File | Settings | File and Code Templates.
 */
public class ConsumerKafka {
    private static final String topicKafka = "firstProject";

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[3]")
                .setAppName("consumer kafka");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1L));
        HashMap<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "coba");

        List<String> list = Arrays.asList(topicKafka);
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(list, props));

        JavaDStream<String> recordData = directStream.map(datas -> {
            System.out.println("<><><><><> " + datas.value());
            return datas.value();
        });

        try {
            recordData.foreachRDD(e -> {
                System.out.println("<><><><> "+e);
                JavaEsSpark.saveJsonToEs(e, "second-insert");
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
