package weather;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.kafka.clients.producer.ProducerRecord;

import redis.clients.jedis.Jedis;

import weather.WeatherObject;

public class WeatherProducer {

	public static String dayURL = "http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/";

	public static String barcelonaKey = "307297";
	public static String madridKey = "308526";
	public static String bilbaoKey = "309382";
	public static String appKey = "Gy3fghjO6t5WrGGOKJRlGdWQaMtzqZJd";
	
	Properties props;
	Jedis jedis;
    
    KafkaProducer<String, Double> producer;
    WeatherProducer() {
       Properties props = new Properties();
       props.put("bootstrap.servers", "localhost:9092");
       // Serializer for conversion the key type to String
       props.put("key.serializer",
                   "org.apache.kafka.common.serialization.StringSerializer");
       // Serializer for conversion the value type to Double
       props.put("value.serializer",
                   "org.apache.kafka.common.serialization.DoubleSerializer");
       producer = new KafkaProducer<>(props);
       jedis = new Jedis("localhost"); 
    }
    
    /**
     * 
     * Main function that gathers WeatherInfo and sends it to "weather-temp-input" and "weather-rain" topics
     */
    
    public static void main(String[] args) {
    
    		WeatherProducer myProducer = new WeatherProducer();
    		String[] cities = {"barcelona","madrid","bilbao"}; 
    		
    		// Fetch all the weather info (city by city), store it in Redis, and produce records for them
    		for (int i=0; i < cities.length; i++) {
    			ArrayList<WeatherObject> list= myProducer.fetchWeatherInfo(cities[i]);
    			myProducer.storeValuesRedis(list, cities[i]);
    			myProducer.produceRecords(cities[i]);
    		}
	    myProducer.stop();
	}
    
    /** Function that makes the calls to accuWeather REST APIs and stores info into WeatherObjects
     * 
     * @param city City we want to get the info for
     * @return WeatherObject containing all the weather info for the specific City
     */
    
    ArrayList<WeatherObject> fetchWeatherInfo(String city) {
    	  	ArrayList<WeatherObject> list = new ArrayList<WeatherObject>();
    	  	String cityKey;
    	  	
    	  	switch (city) {
    	  		case "barcelona": cityKey = barcelonaKey;
    	  		break;
    	  		case "bilbao": cityKey = bilbaoKey;
    	  		break;
    	  		default: cityKey = madridKey;
    	  		break;
    	  	}
    	  	// Make HTTP GET call to AccuWeather API to fetch weather info regarding specific city
    	  	try {
			URL url = new URL(dayURL+cityKey+"?apikey="+appKey);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(
				(conn.getInputStream())));
			 
			// Read the response from the weather API
			String output;
			StringBuilder builder = new StringBuilder();
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
				builder.append(output);
			}
			try{
				JSONArray array = new JSONArray(builder.toString());
				WeatherObject obj;
				
				// From the response of the service, build WeatherObject that will contain the real info
				for(int n = 0; n < array.length(); n++)
				{
				    JSONObject object = array.getJSONObject(n);
				    JSONObject tempObject = object.getJSONObject("Temperature");    
				    obj = new WeatherObject(city, tempObject.get("Value").toString(),object.get("PrecipitationProbability").toString());
				    list.add(obj);
				}
			 }catch(Exception e){
			    System.out.println(e.getMessage());
			 }
			conn.disconnect();
		  } catch (MalformedURLException e) {
			  e.printStackTrace();
	  } catch (IOException e) {
		e.printStackTrace();
	  }
		return list;
	}
    
    
    /** Function to produce records for the collected weather info, stored in Redis
     * 
     * @param city. Ciudad de la que deseamos obtener la información
     */
    
    void produceRecords(String city) {
    	    // Obtener los ultimos 100 eventos almacenados en Redis acerca de informacion meteorologica
	    List<String> listTemp = jedis.lrange("weather-temp" + city, 0 ,100); 
	    List<String> listRain = jedis.lrange("weather-rain" + city, 0 ,100);
	    System.out.println("Elementos en Redis:" + listRain.size());
	    
	    // Recorrer todos los valores para la clave de una ciudad especifica y enviar la info al topic correspondiente
        for (int i = 0; i < listTemp.size(); i++) {
        		System.out.println("Stored string in redis:: "+listRain.get(i));
        		producer.send(new ProducerRecord<String, Double>("weather-temp-input", city , Double.valueOf(listTemp.get(i))),new ProducerCallBack());
        		producer.send(new ProducerRecord<String, Double>("weather-rain", city , Double.valueOf(listRain.get(i))),new ProducerCallBack());
        }
 	}
    
    /** Function that goes through array of Weather objects, and stores info to local Redis
     * 
     * @param localTemp. Array that contains all the weather info fetched from the API
     * @param city. Specific city we want to store the info for
     */
    
    void storeValuesRedis(ArrayList<WeatherObject> localTemp, String city) {
    		// Recorrer array de objetos "Weather" y almacenar la info en Redis local
	    for (int i = 0; i < localTemp.size(); i++) {
	        jedis.lpush("weather-temp" + city, localTemp.get(i).temp); 
	        jedis.lpush("weather-rain" + city, localTemp.get(i).precProb); 
	    }
    }
    
    /**
     * 
     * Implementing callback function to send events to kafka in a Async way
     *
     */
    
    private static class ProducerCallBack implements org.apache.kafka.clients.producer.Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception !=null){
                System.out.println(metadata.topic()+metadata.offset()+metadata.partition());
            }
        }
    }
    
    /**
     * Finally stop the produce, once all the events have been sent
     */
    
	void stop() {
		   producer.close();
	}
}

