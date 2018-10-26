package com.roncoo.eshop.storm.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * kafka消费数据的spout
 */
public class AccessLogKafkaSpout extends BaseRichSpout {

	private static final long serialVersionUID = -6906964955265279027L;

	private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogKafkaSpout.class);

	private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

	private SpoutOutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		startKafkaConsumer();
	}

	private void startKafkaConsumer() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.2.101:2181,192.168.2.102:2181,192.168.2.103:2181");
		props.put("group.id", "eshop-cache-group");
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		ConsumerConfig consumerConfig = new ConsumerConfig(props);

		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
		String topic = "access-log";

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
				.createMessageStreams(topicCountMap);
		
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		
		for(KafkaStream stream: streams){
			//new Thread(new K)
		}
	}

	private class KafkaMessageProcessor implements Runnable{

		@SuppressWarnings("rawtypes")
		private KafkaStream kafkaStream;
		
		@SuppressWarnings("rawtypes")
		public KafkaMessageProcessor(KafkaStream kafkaStream) {
			this.kafkaStream = kafkaStream;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			ConsumerIterator<byte[], byte[]>  it = kafkaStream.iterator();
			while(it.hasNext()){
	        	String message = new String(it.next().message());
	        	LOGGER.info("【AccessLogKafkaSpout中的Kafka消费者接收到一条日志】message=" + message);  
	        	try {
					queue.put(message);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public void nextTuple() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
