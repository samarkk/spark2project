package com.skk.training.kafkastreaming;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
 * This program acts as the producer for the topic "salestopic"
 * in Kafka. You'll need to change the kafka parameters to
 * run it in different configurations 
 */
public class DemoKafkaProducer {

	public static int generateRandomInt(int min, int max) { // Function to
		// generate random integers
		Random rnd = new Random();
		int randomNum = rnd.nextInt((max - min) + 1) + min;
		return randomNum;
	}

	public static float generateRandomFloat() { // Function to
		// generate random floats
		Random rnd = new Random();
		return rnd.nextFloat();
	}

	public static void main(String[] args) throws InterruptedException {
		long events = Long.parseLong("10");

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// props.put("partitioner.class", "kafkaproducer.CustomPartitioner");
		props.put("request.required.acks", "1");

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		while (true) {
			for (long nEvents = 0; nEvents < events; nEvents++) {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date today = Calendar.getInstance().getTime();
				String reportDate = df.format(today);
				int storeId = 1000;
				int[] skus = { 10, 11, 12, 13, 14, 15, 16 };
				int skuId = skus[generateRandomInt(0, 6)];
				float amount = 10 * generateRandomFloat();
				String msg = reportDate + "," + storeId + "," + skuId + "," + amount;
				ProducerRecord<String, String> data = new ProducerRecord<String, String>("salestopic", "somekey", msg);
				System.out.println(msg);
				producer.send(data);
				Thread.sleep(500);
			}
		}
	}
}
