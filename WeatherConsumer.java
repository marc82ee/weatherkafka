package weather;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;
import java.util.List;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.clients.consumer.*;


public class WeatherConsumer {
	Properties props;
	static int threadSize = 10;
    // The producer is a Kafka client that publishes records to the Kafka
    // cluster.
    KafkaConsumer<String, Long> consumer;
    WeatherConsumer() {
    		Properties props = new Properties();
    		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "weather-temp-consumer");
    		props.put("enable.auto.commit", "true");
             // Auto commit interval, kafka would commit offset at this interval.
        props.put("auto.commit.interval.ms", "100");
    		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    		                                          StringDeserializer.class.getName());
    		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    												DoubleDeserializer.class.getName());
    		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    		consumer = new KafkaConsumer<>(props);
    }
    
    public static void main(String[] args) {
		//PropertyConfigurator.configure("log4j.properties");
    		//WeatherConsumer myConsumer  = new WeatherConsumer();
	    //myConsumer.subscribeToTopic("weather-temp-min");
	    //myConsumer.consumeEvents();
	    
	    ExecutorService exServ = Executors.newFixedThreadPool(5);
		for (int i = 0; i < threadSize; i++) {
		Runnable runnableTask = () -> {
			WeatherConsumer myConsumer = new WeatherConsumer();
			myConsumer.subscribeToTopic("weather-temp-output");
			myConsumer.consumeEvents();
		};
		exServ.execute(runnableTask);
		}
	}
    
    void subscribeToTopic(String topic) {
    		List<String> topics = Arrays.asList("weather-temp-output");
    		consumer.subscribe(topics);
    }
    
    void consumeEvents() {
	    	try {
	    		while (true) {
	    			ConsumerRecords<String, Long> records = consumer.poll(100); 
	    			for (ConsumerRecord<String, Long> record : records) { 
	    				System.out.println("something consumed");
	    				System.out.printf("offset = %d, key = %s, value = %f%n",
	    			             record.offset(), record.key(), record.value());
	    		     }//for
	    		}
	    		//while
	    	}finally {
	    		consumer.close();
	    	}
	  }
}
