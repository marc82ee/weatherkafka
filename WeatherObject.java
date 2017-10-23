package weather;

public class WeatherObject {

    public String city = "";
    public String temp = "";
    public String precProb = "";
    //constructor
    public WeatherObject(String city, String temp, String precProb) {
    		this.city = city;
        this.temp = temp;
        this.precProb = precProb;
    }
}
