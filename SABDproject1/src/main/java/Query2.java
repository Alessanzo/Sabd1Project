import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.query2.*;

import java.util.ArrayList;
import java.util.List;

public class Query2 {
    private static String pathToFile = "data/time_series_covid19_confirmed_global.csv";
    private static String contFile = "data/countryContinent.csv";
    private static String outFile = "data/query2out.csv";
    static Integer referencePeriod = 7; //dati calcolati settimanalmente
    static boolean local = false;  //true: esecuzione in locale, false: esecuzione su AWS EMR
    static boolean ignoreHeader = true;  //true: processamento header

    public static void main(String[] args) throws Exception {



        SparkConf conf = new SparkConf()
                .setAppName("SABDproject1Query2");
        JavaSparkContext sc;
        /*Scelta di esecuzone LocaNode o Submit su AWS Cluster con passaggio di parametri*/
        if (local) {
            conf.setMaster("local");
        } else {
            pathToFile = args[0];
            contFile = args[1];
            outFile = args[2];
            ignoreHeader = Boolean.parseBoolean(args[3]);
        }
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");



        StateDataHeaderInfo stateDataHeaderInfo;
        ContinentHeaderInfo continentHeaderInfo;
        String header = null;

        JavaRDD<String> elements = sc.textFile(pathToFile).cache();
        JavaRDD<String> contlines = sc.textFile(contFile).cache(); //file che contiene il mapping tra Nazione e Continente
        /*come query1, flessibilità con parsing headers o risparmio con informazioni manuali*/
        if (ignoreHeader) {
            stateDataHeaderInfo = new StateDataHeaderInfo(0, 1, 4, 3);
            continentHeaderInfo = new ContinentHeaderInfo(0, 1);
        } else {
            header = elements.first();
            stateDataHeaderInfo = StateDataUtils.parseHeader(header);
            String contHeader = contlines.first();
            continentHeaderInfo = StateDataUtils.parseContinentHeader(contHeader);
        }
        /*parsing dei due RDD dei files in base alla informazioni dell'header sugli indici dei campi di interesse*/
        JavaPairRDD<String, String> statecontinents = contlines.mapToPair(line -> StateDataUtils.parseContinentsCsv(line, continentHeaderInfo));
        JavaRDD<StateData> statedata = elements.flatMap(line -> StateDataUtils.parseCsv(line, stateDataHeaderInfo));

        //JavaPairRDD<Double, StateData> statedataregr = statedata.mapToPair(slopeValues);
        //AUDIT
        //statedataregr.collect().forEach(l ->System.out.println("Slope values "+l._2.getState()+":"+l._2.getCountry()+" - "+l._1));

        /*calcolo dei coefficienti angolari delle rette di regressione sugli incrementi giornalieri e ottenimento
        dei primi 100 Stati/Nazioni più colpiti */
        List<Tuple2<Double, StateData>> li = mapToPairSlopeCalculator(statedata)
                .takeOrdered(100, SerializableComparator.serialize((a, b) -> -a._1.compareTo(b._1)));

        /*100 stati più colpiti in forma <Nazione, StateData> in Join con le coppie <Nazione, Contiente>, per ottenere
        * infine il <Continente di appartenenza, Casi>*/
        JavaPairRDD<String, Double[]> topStatesDataJoined = sc.parallelizePairs(li)
                .mapToPair(l -> new Tuple2<>(l._2.getCountry(), l._2))
                .join(statecontinents) //ottenimento coppia (Continente, numero infetti giornalieri del Continente)
                .mapToPair(l -> new Tuple2<>(l._2._2, l._2._1.getInfected()));

        /*casi sono sommati tra loro per Stati/Nazioni con stessa chiave "Continente"*/
        JavaPairRDD<String, Double[]> continentsdata = reduceByKeyStateToContinentAdder(topStatesDataJoined);

        //AUDIT
        //System.out.println("Top Infected Countries with their Continents: "+top.join(statecontinents).count());
        //top.join(statecontinents).values().collect().forEach(l ->System.out.println(l+" - "+l._2));
        /*Calclo delle statistiche settimanali per ogni Contiente*/
        JavaPairRDD<String, Double[][]> weeklymean = mapToPairStatsCalculator(continentsdata, stateDataHeaderInfo);
        //AUDIT
        //weeklymean.collect().forEach(l -> System.out.println(l._2[11][1]));

        List<Tuple2<String, Double[][]>> results = weeklymean.collect();


        /*Salvataggio output su .csv. L'header è usato per generare automaticamente l'header del file di output in base
        * al numero giorni per cui sono stati inseriti i dati, che si riflettono dinamicamente sul numero di colonne*/
        StateDataUtils.writeCSV(results.toArray(), header, stateDataHeaderInfo.getFirstDateIndex(), outFile);


    }


    /*Funzione di MapToPair che usa la libreria Apache Common Math per fare una regressione lineare sugli incrementi
    * giornalieri dei vari Stati ed ottenere lo Slope come chiave della tupla*/
    private static JavaPairRDD<Double, StateData> mapToPairSlopeCalculator(JavaRDD<StateData> statedata){
        return statedata.mapToPair(stateData -> {
            Double[] infections = stateData.getInfected();
            //double[][] differences = new double[infections.length - 1][2];
            double[][] differences = new double[infections.length][2];
            differences[0][0] = (double) 0;
            differences[0][1] = infections[0] - 0;
            for (int i = 0; i < (infections.length - 1); i++) {
                differences[i+1][0] = (double) i;
                differences[i+1][1] = infections[i + 1] - infections[i];
            }
            SimpleRegression linregr = new SimpleRegression(true);
            linregr.addData(differences);
            return new Tuple2<>(linregr.getSlope(), stateData);
        });
    }

    /*Funzione di ReduceByKey che somma tra loro i casi degli Stati/Nazioni associati allo stesso Continente come
    * chiave*/
    private static JavaPairRDD<String, Double[]> reduceByKeyStateToContinentAdder(
            JavaPairRDD<String, Double[]> topStateData){

        return topStateData.reduceByKey((a, b) -> {
            Double[] sum = new Double[a.length];
            for (int i = 0; i < a.length; i++) {
                sum[i] = a[i] + b[i];
            }
            return sum;
        });
    }

    /*MapToPair che per ogni settimana Partendo da Lunedì calcola le statistiche sugli incrementi giornalieri dei
    * Continenti, prendendo la media e la deviazione standard sul numero di giorni della settimana (anche corta se
    * iniziale o finale), e il massimo e il minimo della settimana*/
    private static JavaPairRDD<String, Double[][]> mapToPairStatsCalculator(
            JavaPairRDD<String, Double[]> continentsdata, StateDataHeaderInfo stateDataHeaderInfo) {

        return continentsdata.mapToPair(l -> {
            ArrayList<Double[]> a = new ArrayList<Double[]>();
            Double[] b = new Double[4];
            DescriptiveStatistics stats = new DescriptiveStatistics();

            for (int i = 0; i < l._2.length; i++) {
                if (i == 0) stats.addValue((l._2[i] - 0)); //incremento del primo giorno del dataset riferito a 0
                else stats.addValue((l._2[i] - l._2[i - 1])); //incremento giornaliero
                if ((i + stateDataHeaderInfo.getDaysFromPrevSunday()) % referencePeriod == 0 || i == (l._2.length - 1)) { //settimane contate partendo dall'incremento di lunedì, ma dataset parte da mercoledì
                    b[0] = stats.getMean();
                    b[1] = stats.getStandardDeviation(); //ogni calcolo di incremento settimanale si calcolano le 4 statistiche
                    b[2] = stats.getMax();
                    b[3] = stats.getMin();
                    a.add(b);
                    b = new Double[4];
                    stats.clear();
                }

            }
            Double[][] ed = a.toArray(new Double[a.size()][4]);
            return new Tuple2<>(l._1, ed);
        });
    }


}


