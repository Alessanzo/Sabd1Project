package utils.query1;

import java.io.Serializable;

public class ItaWeeklyHeaderInfo implements Serializable {

    private Integer dateIndex;
    private Integer recoveredIndex;
    private Integer swabsIndex;


    public ItaWeeklyHeaderInfo(Integer dateIndex, Integer recoveredIndex, Integer swabsIndex) {
        this.dateIndex = dateIndex;
        this.recoveredIndex = recoveredIndex;
        this.swabsIndex = swabsIndex;
    }

    public Integer getDateIndex() {
        return dateIndex;
    }

    public Integer getRecoveredIndex() {
        return recoveredIndex;
    }

    public Integer getSwabsIndex() {
        return swabsIndex;
    }
}
