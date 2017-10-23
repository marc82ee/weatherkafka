package weather;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.clients.consumer.*;


// Main class that implements Runnable to keep the consumers running
public class WeatherCons implements Runnable{
	
	private final KafkaConsumer<String, Double> consumer;
	private final List<String> topics;
	private final int id;
	
	/**
	 * Constructor of WeatherCons, that sets-up the main config
	 * @param id
	 * @param groupId
	 * @param topics
	 */
	public WeatherCons(int id,
            			String groupId, 
            			List<String> topics) {
		this.id = id;
		this.topics = topics;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupId);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", DoubleDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);
	}
	
	/**
	 * Main run function that consumes the records from the different topics we are subscribed to
	 */
	@Override
	  public void run() {
	    try {
	      consumer.subscribe(topics);
	      while (true) {
	    	  	ConsumerRecords<String, Double> records = consumer.poll(1000);
	        for (ConsumerRecord<String, Double> record : records) {
	          Map<String, Object> data = new HashMap<>();
	          data.put("partition", record.partition());
	          data.put("offset", record.offset());
	          data.put("value", Double.toString(record.value()));
	          System.out.println(this.id + ": " + data);
	        }
	      }
	    } catch (WakeupException e) {
	      // ignore for shutdown 
	    } finally {
	      consumer.close();
	    }
	  }

	  public void shutdown() {
	    consumer.wakeup();
	  }
	  
	  public static void main(String[] args) { 
		  
		  // Define the number of consumer-threads and set them through ExecutorService
		  int numConsumers = 3;
		  String groupId = "weather-consumer8";
		  List<String> topics = Arrays.asList("weather-temp-min", "weather-temp-max");
		  ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		  final List<WeatherCons> consumers = new ArrayList<>();
		  
		  // Add the consumers threads and submit them to Executor
		  for (int i = 0; i < numConsumers; i++) {
			  WeatherCons consumer = new WeatherCons(i, groupId, topics);
			  consumers.add(consumer);
			  executor.submit(consumer);
		  }

		  // Shutdown all the threads
		  Runtime.getRuntime().addShutdownHook(new Thread() {
		    @Override
		    public void run() {
		      for (WeatherCons consumer : consumers) {
		        consumer.shutdown();
		      } 
		      executor.shutdown();
		      try {
		        executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
		      } catch (InterruptedException e) {
		        e.printStackTrace();
		      }
		    }
		  });
		}

}
