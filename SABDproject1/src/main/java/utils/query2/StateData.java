package utils.query2;

import java.io.Serializable;

public class StateData implements Serializable {

    private String state;
    private String country;
    private Double[] infected;

    public StateData(String state, String country, Double[] infected){
        this.state = state;
        this.country = country;
        this.infected = infected;
    }

    public Double[] getInfected() {
        return infected;
    }

    public String getState() {
        return state;
    }

    public String getCountry() {
        return country;
    }
}

