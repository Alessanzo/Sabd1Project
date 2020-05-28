import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import utils.DateUtils;
import utils.query1.ItaWeeklyHeaderInfo;
import utils.query1.ItaWeeklyMean;
import utils.query1.ItaWeeklyUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Query1 {
    private static String pathToFile = "data/dpc-covid19-ita-andamento-nazionale.csv";
    private static String outFile = "data/query1out.csv";
    static String startingDate = "2020-02-24";
    static boolean local = false; //true: esecuzione in locale, false: esecuzione su AWS EMR
    static boolean ignoreHeader = true;  //true: processamento header

    public static void main(String[] args) throws ParseException, IOException {



        SparkConf conf = new SparkConf()
                .setAppName("SABDproject1Query1");
        JavaSparkContext sc;
        /*Scelta di esecuzone LocaNode o Submit su AWS Cluster con passaggio di parametri*/
        if (local) {
            conf.setMaster("local");
        } else {
            pathToFile = args[0];
            outFile = args[1];
            ignoreHeader = Boolean.parseBoolean(args[2]);
        }
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");



        JavaRDD<String> lines = sc.textFile(pathToFile).cache();

        /*specificando da codice o come argomento dell'App di considerare l'header, flessibilmente l'App seleziona
        tramite un'azione la prima riga del Dataset e parsa le posizioni degli indici restituendole. Altrimenti è
        possibile specificarle staticamente da codice risparmiando uno stage.*/
        ItaWeeklyHeaderInfo itaWeeklyHeaderInfo;
        if (ignoreHeader) {
            itaWeeklyHeaderInfo = new ItaWeeklyHeaderInfo(0, 9, 12);
        } else {
            String header = lines.first();
            itaWeeklyHeaderInfo = ItaWeeklyUtils.parseHeader(header);
        }

        /*parsing delle righe prendendo solo i 3 campi di interesse incapsulandoli in un oggetto*/
        JavaRDD<ItaWeeklyMean> itaLines = lines.flatMap(l -> ItaWeeklyUtils.parseCSV(l, itaWeeklyHeaderInfo));
        //String startingDate = lines.first().getDate();
        //String startingDate = "2020-02-24";
        String previousSunday = DateUtils.previousSunday(startingDate);
        //System.out.println("Il primo giorno del dataset è: "+startingDate+", la Domenica precedente è "+previousSunday);

        /*mapping delle righe con l'identificativo progressivo della settimana a cui appartengono partendo dal Lunedì*/
        JavaPairRDD<Integer, ItaWeeklyMean> weekMappedLines = itaLines.mapToPair(itaLine -> new Tuple2<>(
                (DateUtils.timeDistance(startingDate, itaLine.getDate()) / 7) + 1, itaLine));
                //.cache();

        /*selezionato per ogni settimana il giorno con data massima come giorno di riferimento usato per calcolare l'incremento
        * rispetto alla settimana precedente. Vengono contati i giorni di ogni settimana, per eventuali settimane
        * iniziali o finali corte */
        JavaPairRDD<Integer, ItaWeeklyMean> referenceDays = weekMappedLines.reduceByKey((x,y) ->{
            ItaWeeklyMean z = DateUtils.maxDate(x,y);
            z.setDaysofweek(x.getDaysofweek()+y.getDaysofweek());
            return z;
        });

        /*tramite flatMap ogni giorno è sdoppiato per essere usato sia per il calcolo della media della settimana di
        * appartenenza, sia della successiva*/
        JavaPairRDD<Integer, ItaWeeklyMean> refDaysFlatten = flatMapToPairRefDaysFlattener(referenceDays,
                previousSunday);

        /*raggruppamento delle coppie giorni estremi della settimana e calcolo delle medie giornaliere */
        JavaPairRDD<Integer, ItaWeeklyMean> weeklyMeans = refDaysFlatten.groupByKey()
                .flatMapToPair(new WeeklyMeanCalculator());

        ArrayList<Tuple2<Integer, ItaWeeklyMean>> weeklyMeansArray = new ArrayList<>(weeklyMeans.collect());
        weeklyMeansArray.sort(Comparator.comparingInt(a->a._1));


        /*scrittura dei risultati su file .csv*/
        ItaWeeklyUtils.writeCSV(weeklyMeansArray.toArray(), outFile);


    }



    private static JavaPairRDD<Integer, ItaWeeklyMean> flatMapToPairRefDaysFlattener
            (JavaPairRDD<Integer, ItaWeeklyMean> refDays, String previousSunday){

        return refDays.flatMapToPair(refDay -> {
            ArrayList<Tuple2<Integer, ItaWeeklyMean>> l = new ArrayList<>();
            l.add(new Tuple2<>(refDay._1, refDay._2));
            l.add(new Tuple2<>(refDay._1+ 1, refDay._2));
            if (refDay._1 == 1){ /*il giorno di riferimento della prima settimana viene confrontato con un valore fittizio "0"*/
                l.add(new Tuple2<>(refDay._1, new ItaWeeklyMean(previousSunday, 0, 0, l.size())));
            }
            return l.iterator();
        });
    }

    private static class WeeklyMeanCalculator implements PairFlatMapFunction
            <Tuple2<Integer,Iterable<ItaWeeklyMean>>, Integer, ItaWeeklyMean> {

        @Override
        public Iterator<Tuple2<Integer, ItaWeeklyMean>> call(Tuple2<Integer,Iterable<ItaWeeklyMean>> rangeDays)
                throws Exception {

            ArrayList<Tuple2<Integer, ItaWeeklyMean>> l = new ArrayList<>();
            List<ItaWeeklyMean> list = new ArrayList<>();
            rangeDays._2.forEach(list::add);

            if (list.size() > 1){ //uso di groupBy consente di scartare il doppione dell'ultimo giorno, se l'Iterator contiene un solo elemento

                ItaWeeklyMean prevweekref = DateUtils.minDate(list.get(0),list.get(1)); //usato per ottenere la data del giorno iniziale della settimana
                ItaWeeklyMean weekref = DateUtils.maxDate(list.get(0),list.get(1)); //usato per mantenere la numerazione ordinata delle settimane
                float daysofweek = (float) weekref.getDaysofweek();

                float recmean = (Math.abs(weekref.getRecovered() - prevweekref.getRecovered())) / daysofweek; //media dei guariti sul numero di giorni
                float swabmean = (Math.abs(weekref.getSwabtests() - prevweekref.getSwabtests())) / daysofweek;//medi dei tamponi sul numero di giorni
                ItaWeeklyMean finalweekmean = new ItaWeeklyMean(DateUtils.nextDay(prevweekref.getDate()),
                        recmean, swabmean );

                l.add(new Tuple2<>(rangeDays._1, finalweekmean));
            }
            return l.iterator();

        }
    }
}

