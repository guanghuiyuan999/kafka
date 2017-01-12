import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 接收数据
 * 接收到: message: 10
 接收到: message: 11
 接收到: message: 12
 接收到: message: 13
 接收到: message: 14
 * @author zm
 *
 */
public class kafkaConsumer extends Thread{

	private String topic;

	public kafkaConsumer(String topic){
		super();
		this.topic = topic;
	}


	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
		Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
		ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
		while(iterator.hasNext()){
			String message = new String(iterator.next().message());
			System.out.println("接收到: " + message);
		}
	}

	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();
		// 172.16.1.46:2181,172.16.1.46:2182,172.16.1.46:2183
//		properties.put("zookeeper.connect", "172.16.1.90:9092,172.16.1.91:9092");//声明zk
//		properties.put("group.id", "group1");// group1 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
		properties.put("zookeeper.connect", "172.16.1.46:2181,172.16.1.46:2182,172.16.1.46:2183");//声明zk
		properties.put("group.id", "group1");// group1 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}


	public static void main(String[] args) {
//		new kafkaConsumer("babel.crawl.result.test").start();// 使用kafka集群中创建好的主题 test
		new kafkaConsumer("abc").start();// 使用kafka集群中创建好的主题 test

	}

}