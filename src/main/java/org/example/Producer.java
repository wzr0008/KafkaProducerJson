package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class Producer {
    private static final String TOPIC="rui";
    private static final String broker_list="localhost:9092";
    private static KafkaProducer<String,String> producer=null;
    private static ObjectMapper mapper=new ObjectMapper();

    private static Properties initConfig(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker_list);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"lz4");
        return properties;
    }
    static {
        Properties properties = initConfig();
        producer=new KafkaProducer<>(properties);
    }
    public static void main(String[] args) throws JsonProcessingException {

        ProducerRecord<String, String> record=null;
        for(int i=0;i<1000;i++){
            List<Map<String,String>> list=new ArrayList<>();
            Map<String,String> map = new HashMap<>();
            map.put("name","Rui wang");
            map.put("id","value "+i);
            map.put("Country","China");
            list.add(map);

            record=new ProducerRecord<>(TOPIC,mapper.writeValueAsString(list));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println("Hello ");
                    }
                }
            });
        }
        producer.close();
    }
}
