package utils.query2;

import java.io.Serializable;

public class StateDataHeaderInfo implements Serializable {

    private Integer stateIndex;
    private Integer countryIndex;
    private Integer firstDateIndex;
    private Integer daysFromPrevSunday;

    public StateDataHeaderInfo(Integer stateIndex, Integer countryIndex, Integer firstDateIndex, Integer daysFromPrevSunday) {
        this.stateIndex = stateIndex;
        this.countryIndex = countryIndex;
        this.firstDateIndex = firstDateIndex;
        this.daysFromPrevSunday = daysFromPrevSunday;
    }


    public Integer getStateIndex() {
        return stateIndex;
    }

    public Integer getCountryIndex() {
        return countryIndex;
    }

    public Integer getFirstDateIndex() {
        return firstDateIndex;
    }
    public Integer getDaysFromPrevSunday() {
        return daysFromPrevSunday;
    }
}
