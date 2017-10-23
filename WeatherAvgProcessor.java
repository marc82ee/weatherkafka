package weather;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import org.apache.kafka.common.serialization.*;


//import org.rocksdb.RocksDB;
//import org.rocksdb.RocksDBException;

import java.util.Locale;
import java.util.Properties;

public class WeatherAvgProcessor{
	
	public static void main(String[] args) throws Exception {
		//private final KeyValueStore<String, Integer> kvStore;
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		Serde<String> stringSerde = Serdes.String();
		Serde<Double> doubleSerde = Serdes.Double();
		//Tuple2 tuple =  new Tuple2<>(0L, 0.0d);
		//Serde<Tuple2> tupleSerde = Serdes.serdeFrom(Tuple2.class);
		//Tuple2 tuple =  new Tuple2<>(0L, 0.0d);
		//final Serde<Tuple2> tupleSerde = Serdes.serdeFrom(tuple);
        	
        final KStreamBuilder builder = new KStreamBuilder();
 
        //KStream<String, String> source = builder.stream("weather");
        KStream<String, Double> stream = builder.stream(stringSerde, doubleSerde, "weather-temp-input");
        stream.foreach((key, value) -> System.out.println(key + " => " + value));
        stream.to("weather-temp-output");
        
        //stream.groupBy((key, value) -> key).count("Counts").to(Serdes.String(),Serdes.Long(),"tiempoC-output");
        /*final KGroupedStream<String, Double> groupedStream = stream.groupByKey();
        final KTable<String,Tuple2<Long,Double>> countAndSum = groupedStream.aggregate(
                    new Initializer<Tuple2<Long, Double>>() {
                        @Override
                        public Tuple2<Long, Double> apply() {
                            return new Tuple2<>(0L, 0.0d);
                        }
                    },
                    new Aggregator<String, Long, Tuple2<Long, Double>>() {
                        @Override
                        public Tuple2<Long, Double> apply(final String key, final Long value, final Tuple2<Long, Double> aggregate) {
                                ++aggregate.value1;
                                aggregate.value2 += value;
                                return aggregate;
                        }
                    },
                    Serdes.serdeFrom(Tuple2.class));
        //System.out.println("I aqui.." +groupedStream.toString());
        
       // KTable<String, Double> aggregatedStream = groupedStream.aggregate(
        	//    () -> 0.0d, /* initializer */
        //	    (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
        //	    Serdes.Double(), /* serde for aggregate value */
        	//    "aggregated-stream-store" /* state store name */);
        
        
        KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);
        kafkaStreams.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
      
	}
	
	private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Counter");
        props.put("group.id", "test-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-avg-bcn");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      
        return props;
    }
	
	class Tuple2<T1, T2> {
		  public T1 value1;
		  public T2 value2;
		 
		  Tuple2(T1 v1, T2 v2) {
		    value1 = v1;
		    value2 = v2;
		  }
		}
	
}