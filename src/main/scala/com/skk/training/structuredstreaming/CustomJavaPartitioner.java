package com.skk.training.structuredstreaming;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import kafka.utils.VerifiableProperties;

public class CustomJavaPartitioner implements Partitioner {
	public CustomJavaPartitioner(VerifiableProperties props) {

	}

	public CustomJavaPartitioner() {
		// TODO Auto-generated constructor stub
	}

	public int getNumPartition(String stockName) {
		int partNoToRet = 0;
		String stockBeg = stockName.substring(0, 1).toString().toUpperCase();

		if (stockBeg.compareTo("L") <= 0) {
			partNoToRet = 0;
		} else if (stockBeg.compareTo("L") > 0 && stockBeg.compareTo("S") <= 0) {
			partNoToRet = 1;
		} else
			partNoToRet = 2;
		return partNoToRet;
	}

	// public int partition(Object key, int a_numPartitions) {
	// return getNumPartition((String) key);
	// }

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		return getNumPartition((String) key);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
