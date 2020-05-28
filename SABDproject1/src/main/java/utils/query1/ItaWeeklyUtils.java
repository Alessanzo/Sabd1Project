package utils.query1;

import scala.Tuple2;
import utils.DateUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

public class ItaWeeklyUtils {

    /*In base alle informazioni sull'header ogni riga è parsata e i campi Stato, Nazione e il vettore di casi nei
     * vari giorni è individuato e salvato in un oggetto ItaWeeklyMean*/
    public static Iterator<ItaWeeklyMean> parseCSV(String csvLine, ItaWeeklyHeaderInfo itaWeeklyHeaderInfo)
            throws ParseException {

        ArrayList<ItaWeeklyMean> l = new ArrayList<>();
        String[] fields = csvLine.split(",");
        try {
            Double.parseDouble(fields[itaWeeklyHeaderInfo.getRecoveredIndex()]);
        } catch (Exception p){
            return l.iterator();
        }
        l.add(new ItaWeeklyMean(
                DateUtils.reformatDate(fields[itaWeeklyHeaderInfo.getDateIndex()]),
                //csvValues[0], //date
                Integer.parseInt(fields[itaWeeklyHeaderInfo.getRecoveredIndex()]), //recovered
                Integer.parseInt(fields[itaWeeklyHeaderInfo.getSwabsIndex()]),
                1//swab tests
        ));
        return l.iterator();
    }

    /*In base al matching delle colonne di interesse con le stringhe "guariti", "tamponi" e "data" si prelevano gli indici
     * di colonna*/
    public static ItaWeeklyHeaderInfo parseHeader(String header) throws ParseException {
        Integer dateIndex = -1;
        Integer recoveredIndex = -1;
        Integer swabsIndex = -1;
        String[] fields = header.split(",");
        for (int i=0;i<fields.length;i++) {
            if (fields[i].contains("data")) {
                dateIndex = i;
            } else if (fields[i].contains("guariti")) {
                recoveredIndex = i;
            } else if (fields[i].contains("tamponi")) {
                swabsIndex = i;
            }
        }
        if(dateIndex >= 0 && recoveredIndex >= 0 && swabsIndex >= 0){
            return new ItaWeeklyHeaderInfo(dateIndex, recoveredIndex, swabsIndex);
        }
        else throw new ParseException("Date, Recovered or Swabs header fields not found",0);
    }


    /*generazione del file partendo dai dati, visualizzando anche il numero della settimana e il primo giorno
    della settimama*/
    public static void writeCSV(Object[] list, String outpath) throws IOException {
        FileWriter fout = new FileWriter(outpath);
        String header = "settimana,giorno iniziale,media giornaliera guariti,media giornaliera tamponi\n";
        fout.write(header);
        for (Object elem:list) {
            String content = "";
            Tuple2<Integer, ItaWeeklyMean> line = (Tuple2<Integer, ItaWeeklyMean>)  elem;
            content += line._1+",";
            content += line._2.getDate()+",";
            content += line._2.getMeanrec()+",";
            content += line._2.getMeanswab();
            content+="\n";
            fout.write(content);
        }
        fout.close();
    }

}
