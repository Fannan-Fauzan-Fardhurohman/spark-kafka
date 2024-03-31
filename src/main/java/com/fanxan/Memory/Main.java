package com.fanxan.Memory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * Project SparkKafka
 * User: Fannan
 * Date: 3/31/2024
 * Time: 10:06 AM
 * To change this template use File | Settings | File and Code Templates.
 */
public class Main {
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();
        SparkConf sparkConf = new SparkConf().setMaster("local[3]").setAppName("Produce kafka");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("INFO");

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        JavaRDD<String> javaRDD = javaSparkContext.textFile("C:\\Users\\fanna\\Documents\\PersonalProject\\SparkKafka\\src\\main\\resources\\products.json").flatMap(data -> {
            System.out.println("><><><><><><>");
            JsonNode jsonNode = objectMapper.readTree(data);
            ArrayList<String> objects = new ArrayList<>();
            for (JsonNode data2 : jsonNode) {
                objects.add(objectMapper.writeValueAsString(data2));
            }

            return objects.iterator();
        });

        try{
            List<String> collect = javaRDD.collect();
            for(String datas: collect){
                JsonNode jsonNode = objectMapper.readTree(datas);
                for(JsonNode dataReal : jsonNode){
                    String dataRealFix = objectMapper.writeValueAsString(dataReal);
                    System.out.println(dataReal);
                    ProducerRecord<String, String> producerData = new ProducerRecord<>("firstProject",dataRealFix);
                    producer.send(producerData);

                }

            }


        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
