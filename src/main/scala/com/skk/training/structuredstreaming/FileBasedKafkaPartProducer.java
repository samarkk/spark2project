package com.skk.training.structuredstreaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FileBasedKafkaPartProducer {
	public static void main(String[] args) throws InterruptedException, IOException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("partitioner.class", "com.skk.training.structuredstreaming.CustomJavaPartitioner");
		props.put("request.required.acks", "1");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		File folder = new File(args[0]);
		File[] loF = folder.listFiles();
		for (File file : loF) {
			System.out.println(file.getName());
			BufferedReader reader = new BufferedReader(new FileReader(file));
			reader.readLine();
			String line;
			while ((line = reader.readLine()) != null) {
				// System.out.println(line.substring(0, line.indexOf(",")) + " , " + line);
				String rkey = line.substring(0, line.indexOf(","));

				ProducerRecord<String, String> record = new ProducerRecord<String, String>("nsecmdpart", rkey, line);
				producer.send(record);
				Thread.sleep(Integer.parseInt(args[1]));
			}
		}
	}
}
