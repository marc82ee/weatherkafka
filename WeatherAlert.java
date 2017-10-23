package weather;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class WeatherAlert {
	
	// A processor that sends an alert when temperature hits a certain limit to configurable email address
	public static class WeatherTemperatureAlert implements Processor<String, Double> {

	  private final String emailAddress;
	  private ProcessorContext context;

	  public WeatherTemperatureAlert(String emailAddress) {
	    this.emailAddress = emailAddress;
	  }

	  @Override
	  public void init(ProcessorContext context) {
	    this.context = context;
	  }

	  @Override
	public void process(String city, Double temp) {
		  System.out.println("La prob de lluvia ha llegado al limite en " + city +", con un valor de: "+ temp +". Voy a enviar un mail a " + this.emailAddress);
	  }

	  @Override
	  public void punctuate(long timestamp) {
	    // Stays empty.  In this use case there would be no need for a periodical action of this processor.
	  }

	  @Override
	  public void close() {
	    // Any code for clean up would go here.  This processor instance will not be used again after this call.
	  }

	}
	
	public static void main(String[] args) throws Exception {

		InputStream input = new FileInputStream("./resources/config.properties");
		Properties config = new Properties();
		config.load(input);
		
		// Fetch config values from config
		Double alertLimit = Double.valueOf(config.getProperty("weather.alert"));
		String alertCity = config.getProperty("weather.city");
		System.out.println("Coming limit from config" + alertLimit);
		
		// Serdes to store aggregated values
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		Serde<String> stringSerde = Serdes.String();
		Serde<Double> doubleSerde = Serdes.Double();
		
		// Using a simple Stream SDK builder
		final KStreamBuilder builder = new KStreamBuilder();
		
		// Filter the values received according to alert values
		KStream<String, Double> tempStream = builder.stream(stringSerde, doubleSerde, "weather-rain");
		tempStream.filter(
	            new Predicate<String, Double>() {
	                public boolean test(String city, Double temp) {
	                  return temp >= alertLimit && city == alertCity;
	                }
	              })
	           .process( // In case filter query returns results, handle it through process()
	             new ProcessorSupplier<String, Double>() {
	               public Processor<String, Double> get() {
	                 return new WeatherTemperatureAlert("alerts@yourcompany.com");
	               }
	             });

        // Set up the Stream and start it
        KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);
        kafkaStreams.start();
	}
	
	private static Properties getProperties() {
        Properties props = new Properties();
        props.put("group.id", "weather-rain");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-rain3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      
        return props;
    }
}
