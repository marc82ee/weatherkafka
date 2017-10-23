package weather;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

public class WeatherMinProcessor {

    public static void main(String[] args) {    
        TopologyBuilder builder = new TopologyBuilder();
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        // Define the 2 StateStore to keep current min-max values
        StateStoreSupplier minStore = Stores.create("Min")
                .withStringKeys()
                .withDoubleValues()
                .inMemory()
                .build();
        StateStoreSupplier maxStore = Stores.create("Max")
                .withStringKeys()
                .withDoubleValues()
                .inMemory()
                .build();

        // Build the topology builder that connects the input with the 2 processors and its corresponding "Sinks"
        builder.addSource("source", "weather-temp-input")
               .addProcessor("processMin", () -> new MinProcessor(), "source")
               .addProcessor("processMax", () -> new MaxProcessor(), "source")
               .addStateStore(minStore, "processMin")
               .addStateStore(maxStore, "processMax")
               .addSink("sink", "weather-temp-min", "processMin")
        		   .addSink("sink2", "weather-temp-max", "processMax");

        // Setup the Kafka-Stream and start it
        KafkaStreams streams = new KafkaStreams(builder,streamsConfig);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("group.id", "weather-processor");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-processor2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      
        return props;
    }
    
    /**
     * Class that implements Processor interface and figures out the current min value
     * @author marc
     *
     */
    public static class MinProcessor implements Processor<String,Double> {

        private ProcessorContext context;
        private KeyValueStore<String, Double> kvStore;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.context.schedule(1000);
            this.kvStore = (KeyValueStore<String,Double>) context.getStateStore("Min");
        }

        @Override
        public void process(String key, Double value) {
        		Double minValue = kvStore.get(key);
            System.out.printf("key: %s and min value %f \n", key, minValue);

            if (minValue == null || value < minValue) {
                this.kvStore.put(key, value);
                context.forward(key,value);
                context.commit();
            }
        }

        @Override
        public void punctuate(long timestamp) {}

        @Override
        public void close() {
            this.kvStore.close();
        }
    }
    
    /**
     * Class that implements Processor interface and figures out the current max value
     * @author marc
     *
     */
    public static class MaxProcessor implements Processor<String,Double> {

        private ProcessorContext context;
        private KeyValueStore<String, Double> kvStore;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.context.schedule(1000);
            this.kvStore = (KeyValueStore<String,Double>) context.getStateStore("Max");
        }

        @Override
        public void process(String key, Double value) {
        		Double maxValue = kvStore.get(key);
            System.out.printf("key: %s and max value %f \n", key, maxValue);

            if (maxValue == null || value > maxValue) {
                this.kvStore.put(key, value);
                context.forward(key,value);
                context.commit();
            }
        }

        @Override
        public void punctuate(long timestamp) {}

        @Override
        public void close() {
            this.kvStore.close();
        }
    }
   
}
