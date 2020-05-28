package utils.query1;

import java.io.Serializable;

public class ItaWeeklyMean implements Serializable {
    private String date;
    private Integer recovered;
    private Integer swabtests;
    private Float meanrec;
    private Float meanswab;
    private Integer daysofweek;

    public ItaWeeklyMean(String date, Integer recovered, Integer swabtests, Integer daysofweek) {
        this.date = date;
        this.recovered = recovered;
        this.swabtests = swabtests;
        this.daysofweek = daysofweek;
    }

    public ItaWeeklyMean(String date, Float meanrec, Float meanswab) {
        this.date = date;
        this.meanrec = meanrec;
        this.meanswab = meanswab;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Integer getRecovered() {
        return recovered;
    }

    public void setRecovered(Integer recovered) {
        this.recovered = recovered;
    }

    public Integer getSwabtests() {
        return swabtests;
    }

    public void setSwabtests(Integer swabtests) {
        this.swabtests = swabtests;
    }

    public Float getMeanrec() {
        return meanrec;
    }

    public void setMeanrec(Float meanrec) {
        this.meanrec = meanrec;
    }

    public Float getMeanswab() {
        return meanswab;
    }

    public void setMeanswab(Float meanswab) {
        this.meanswab = meanswab;
    }

    public Integer getDaysofweek() {
        return daysofweek;
    }

    public void setDaysofweek(Integer daysofweek) {
        this.daysofweek = daysofweek;
    }
}
