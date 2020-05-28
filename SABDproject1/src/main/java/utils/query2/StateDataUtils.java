package utils.query2;

import scala.Tuple2;
import utils.DateUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class StateDataUtils {

    /*In base al matching delle colonne di interesse con le stringhe "Country", "State" e la prima della serie di
     colonne dei dati si prelevano gli indici di colonna*/
    public static StateDataHeaderInfo parseHeader(String header) throws Exception{
        String[] fields = header.split(",");
        Integer stateIndex = -1;
        Integer countryIndex = -1;
        Integer firstDateIndex = -1;
        Integer daysFromPrevSunday = -1;
        
        for (int i=0;i<fields.length;i++){
            if (fields[i].contains("State")){
                stateIndex = i;
            }
            else if (fields[i].contains("Country")){
                countryIndex = i;
            }
            else if (DateUtils.isValiDate(fields[i], "MM/dd/yy")){
                firstDateIndex = i;
                daysFromPrevSunday = DateUtils.timeDistance(fields[i], DateUtils.previousSunday(fields[i], "M/dd/uu"), "M/dd/uu");
                break;
            }
        }
        if (stateIndex >= 0 && countryIndex>=0 && firstDateIndex >= 0){
            return new StateDataHeaderInfo(stateIndex, countryIndex, firstDateIndex, daysFromPrevSunday);
        }
        else throw new ParseException("Country, State or Data indexes not found",0);
    }


    /*In base alle informazioni sull'header ogni riga è parsata e i campi Stato, Nazione e il vettore di casi nei
    * vari giorni è individuato e salvato in un oggetto StateData*/
    public static Iterator<StateData> parseCsv(String line, StateDataHeaderInfo stateDataHeaderInfo){
        ArrayList<StateData> l = new ArrayList<>();
        String[] fields = line.split(",");
        try {
            Double.parseDouble(fields[stateDataHeaderInfo.getFirstDateIndex()]);
        } catch (Exception p){
            return l.iterator();
        }
        Double[] infections = Arrays.stream(Arrays.copyOfRange(fields, stateDataHeaderInfo.getFirstDateIndex(), fields.length))
                .map(Double::valueOf).toArray(Double[]::new);
        for (int i = 1; i < infections.length; i++) {
            if (infections[i] < infections[i - 1]) {
                int j = i + 1;
                while (j < infections.length) {
                    if (infections[j] >= infections[i - 1]) {
                        double a = infections[i];
                        infections[i] = Math.floor((infections[i - 1] + infections[j]) / 2.0);
                        //System.out.println("TROVATOH IN NAZIONE: "+fields[0] + " " + fields[1] + "VALORE VECCHIO: "+ a+", VALORE NUOVO:"+infections[i]);
                        break;
                    }
                    j++;
                }
                if (j == infections.length) {
                    double a = infections[i];
                    infections[i] = infections[i - 1];
                    //System.out.println("TROVATOH IN NAZIONE: "+fields[0] + " " + fields[1] + "VALORE VECCHIO: "+ a+", VALORE NUOVO:"+infections[i]);
                }
            }
        }
        l.add(new StateData(fields[stateDataHeaderInfo.getStateIndex()], fields[stateDataHeaderInfo.getCountryIndex()], infections));
        return l.iterator();
    }

    /*In base al matching delle colonne di interesse con le stringhe "country" e "continent" si prelevano gli indici
    * di colonna*/
    public static ContinentHeaderInfo parseContinentHeader(String header) throws ParseException{
        String[] fields = header.split(",");
        Integer countryIndex = -1;
        Integer continentIndex = -1;

        for (int i=0;i<fields.length;i++){
            if (fields[i].equals("country")){
                countryIndex = i;
            }
            else if (fields[i].contains("continent")){
                continentIndex = i;
            }
        }
        if (countryIndex>=0 && continentIndex >= 0){
            return new ContinentHeaderInfo(countryIndex, continentIndex);
        }
        else throw new ParseException("Country, State or Data indexes not found",0);
    }

    /*in base alle informazioni sugli indici si prelevano le colonne di interesse*/
    public static Tuple2<String,String> parseContinentsCsv(String line, ContinentHeaderInfo header){
        String[] fields = line.split(",");
        String state = "none";
        String continent = "none";
        if(fields.length <= header.getContinentIndex()){
            return new Tuple2<>(state, continent);
        }
        state = fields[header.getCountryIndex()];
        continent = fields[header.getContinentIndex()];
        return new Tuple2<>(state, continent);
    }

    /*Iterativamente l'header del .csv di output è generato partendo da quello di input in base ai giorni del Dataset
    * iniziale, per suddividere dinamicamente le settimane. Infine vengono scritti i dati nel formato
    * (sett1 media, sett1, stdev, sett1 max, sett1 min, sett2 media....)*/
    public static void writeCSV(Object[] list, String header, Integer firstdataindex, String outpath)
            throws IOException {

        FileWriter fout = new FileWriter(outpath);

        if (header != null) { //header considerato, e header di output costruito dinamicamente
            String[] fields = header.split(",");
            String[] dates = Arrays.copyOfRange(fields, firstdataindex, fields.length);
            String firstmonday = DateUtils.nextDay(DateUtils.previousSunday(fields[firstdataindex], "M/dd/uu"));
            String csvheader = "";
            csvheader = "continente,sett " + firstmonday +
                    " media,sett " + firstmonday +
                    " stdev,sett " + firstmonday +
                    " max,sett " + firstmonday + " min";

            int i = 5;
            while (i < dates.length) {
                csvheader += ",sett " + dates[i] + " media" +
                        ",sett " + dates[i] + " stdev" +
                        ",sett " + dates[i] + " min" +
                        ",sett " + dates[i] + " max";

                i += 7;
            }
            csvheader += "\n";
            fout.write(csvheader);
        }
        else{ // hardcoded header con headerignore, non costruito dinamicamente
            Tuple2<String, Double[][]> e = (Tuple2<String, Double[][]>) list[0];
            String[] dates = {"1/20/20","1/27/20","2/3/20","2/10/20","2/17/20","2/24/20",
                    "3/2/20","3/9/20","3/16/20","3/23/20","3/30/20","4/6/20","4/13/20",
                    "4/20/20","4/27/20","5/4/20","5/11/20"};
            String csvheader = "continente";
            for (String d:dates){
                csvheader += ",sett " + d + " media" +
                        ",sett " + d + " stdev" +
                        ",sett " + d + " min" +
                        ",sett " + d + " max";
            }
            csvheader += "\n";
            fout.write(csvheader);
            //fout.write(HEADER);
        }
        for (Object elem:list) {
            String content = "";
            Tuple2<String, Double[][]>  line = (Tuple2<String, Double[][]>)  elem;
            content += line._1;
            for (Double[] row:line._2){
                for (Double data: row){
                    content+= ","+Double.toString(data);
                }
            }
            content+="\n";
            fout.write(content);


        }
        fout.close();
    }
}
