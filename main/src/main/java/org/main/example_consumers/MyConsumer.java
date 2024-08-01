package org.main.example_consumers;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MyConsumer {
	public static void main(String[] args) {
		String topic = "pos1";
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "Consumer application");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "grp2");
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		
		
		Consumer<Integer,String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		while(true) {
		ConsumerRecords<Integer,String> records = consumer.poll(100);
		
		for(ConsumerRecord<Integer, String> record:records) {
			System.out.println("key :"+record.key() + " partition : "+record.toString()+ "and offset : "+record.offset());
		}
		}
		
	}

}
