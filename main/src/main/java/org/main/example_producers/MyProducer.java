package org.main.example_producers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Properties;

public class MyProducer {
	public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG,"Prod application");

        try {
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        for(int i=0;i<10;i++) {
        RecordMetadata metadata = producer.send(new ProducerRecord<>("test1",i,"hello")).get();
        System.out.println(metadata.partition());
        System.out.println(metadata.offset());
        }

        producer.flush();
        producer.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }

    }
}
