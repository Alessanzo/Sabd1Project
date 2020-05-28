package utils;

import utils.query1.ItaWeeklyMean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Date;

public class DateUtils {

    public static String reformatDate(String date) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        SimpleDateFormat output = new SimpleDateFormat("yyyy-MM-dd");
        Date d = sdf.parse(date);
        return output.format(d);

    }

    public static int timeDistance(String date1, String date2){
        LocalDate d1 = LocalDate.parse(date1);
        LocalDate d2 = LocalDate.parse(date2);
        return (int) Math.abs(ChronoUnit.DAYS.between(d1,d2));
    }

    public static int timeDistance(String date1, String date2, String pattern){
        LocalDate d1 = LocalDate.parse(date1, DateTimeFormatter.ofPattern(pattern));
        LocalDate d2 = LocalDate.parse(date2);
        return (int) Math.abs(ChronoUnit.DAYS.between(d1,d2));
    }

    public static String previousSunday(String date){
        LocalDate d = LocalDate.parse(date);
        LocalDate prev = d.with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
        return prev.toString();
    }

    public static String previousSunday(String date, String pattern){
        LocalDate d = LocalDate.parse(date, DateTimeFormatter.ofPattern(pattern));
        LocalDate prev = d.with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
        return prev.toString();
    }

    public static String nextDay(String date){
        LocalDate d = LocalDate.parse(date);
        return d.plusDays(1).toString();
    }

    public static ItaWeeklyMean minDate(ItaWeeklyMean date1, ItaWeeklyMean date2)throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date firstDate = sdf.parse(date1.getDate());
        Date secondDate = sdf.parse(date2.getDate());
        if(firstDate.compareTo(secondDate) < 0){
            return date1;
        }
        else
            return date2;
    }

    public static ItaWeeklyMean maxDate(ItaWeeklyMean date1, ItaWeeklyMean date2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date firstDate = null;
        Date secondDate = null;
        try {
            firstDate = sdf.parse(date1.getDate());
            secondDate = sdf.parse(date2.getDate());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if(firstDate.compareTo(secondDate) < 0){
            return date2;
        }
        else
            return date1;
    }

    public static boolean isValiDate(String date, String pattern){
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        try {
            dateFormat.parse(date);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }
}
