package utils.query2;

import java.io.Serializable;

public class ContinentHeaderInfo implements Serializable {
    private Integer countryIndex;
    private Integer continentIndex;

    public ContinentHeaderInfo(Integer countryIndex, Integer continentIndex) {
        this.countryIndex = countryIndex;
        this.continentIndex = continentIndex;
    }

    public Integer getCountryIndex() {
        return countryIndex;
    }

    public Integer getContinentIndex() {
        return continentIndex;
    }
}
