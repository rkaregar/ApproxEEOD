package ca.uwindsor.cs.mkargar.od;

import ca.uwindsor.cs.mkargar.od.metanome.ORDERLhsRhs;
import org.apache.lucene.util.OpenBitSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by Mehdi on 6/20/2016.
 */
public class MainClass {

    //reasonable datasets: abalone, chess, bridges, hepatitis

    //public static String ConfigFileName = "/home/mkargar/Mehdi/OD/config.txt";
//    public static String ConfigFileName = "D:/Code/Datasets/OD/config.txt";
    public static String ConfigFileName = "config.txt";


    //export CLASSPATH=.:/home/mkargar/Mehdi/OD/Code/lib/*:

    public static String DatasetFileName = "";
    public static String AlgorithmName = "";

    public static int MaxRowNumber = 1000000;
    public static int MaxColumnNumber = 1000;
    public static int RunTimeNumber = 6;

    public static String cvsSplitBy = ",";

    public static boolean Prune = true;

    public static boolean FirstTimeRun = true;

    public static boolean InterestingnessPrune = false;

    public static long InterestingnessThreshold = 10000000;

    public static int topk = 100;

    public static boolean BidirectionalTrue = false;

    public static boolean RankDoubleTrue = true;

    public static boolean ReverseRankingTrue = false;

    public static boolean BidirectionalPruneTrue = false;

    public static boolean DoubleAttributes = false;

    public static boolean FindUnionBool = false;

    public static Random Rand;

    public static boolean reverseRank = true;

    public static int ReverseRankingPercentage = 90; //larger than 100 will always reverse it, negative will be regular ranking

    public static List<String> odList = new ArrayList<String>();



    public static Boolean approxODBoolean = true;
    public static Integer violationThreshold = 0; // TODO: 3/17/20 set appropriate value


    public static void main(String[] args) {

        if(FindUnionBool) {
            findUnion();
            return;
        }

        printTime();

        Rand = new Random(19999);

        //TANE      FastOD      ORDER
        //Comma     Tab
        try {
            BufferedReader br = new BufferedReader(new FileReader(ConfigFileName));

            DatasetFileName = br.readLine().trim();
            AlgorithmName = br.readLine().trim();

            MaxRowNumber = Integer.parseInt(br.readLine().trim());
            MaxColumnNumber = Integer.parseInt(br.readLine().trim());
            RunTimeNumber = Integer.parseInt(br.readLine().trim());

            cvsSplitBy = br.readLine().trim();
//            String lineSeparator = br.readLine().trim();
//            if(lineSeparator.equals("Comma"))
//                cvsSplitBy = ",";
//            if(lineSeparator.equals("Tab"))
//                cvsSplitBy = "\t";

            String pruneS = br.readLine().trim();
            if(pruneS.equals("PruneFalse"))
                Prune = false;

            String InterestingnessPruneS = br.readLine().trim();
            if(InterestingnessPruneS.equals("InterestingnessPruneTrue"))
                InterestingnessPrune = true;

            InterestingnessThreshold = Long.parseLong(br.readLine().trim());

            topk = Integer.parseInt(br.readLine().trim());

            String BidirectionalTrueS = br.readLine().trim();
            if(BidirectionalTrueS.equals("BidirectionalTrue"))
                BidirectionalTrue = true;

            String RankDoubleTrueS = br.readLine().trim();
            if(!RankDoubleTrueS.equals("RankDoubleTrue"))
                RankDoubleTrue = false;

            String ReverseRankingTrueS = br.readLine().trim();
            if(ReverseRankingTrueS.equals("ReverseRankingTrue"))
                ReverseRankingTrue = true;

            String BidirectionalPruneTrueS = br.readLine().trim();
            if(BidirectionalPruneTrueS.equals("BidirectionalPruneTrue"))
                BidirectionalPruneTrue = true;

            ReverseRankingPercentage = Integer.parseInt(br.readLine().trim());

        }catch (Exception ex){
            ex.printStackTrace();
            return;
        }

        TaneAlgorithm taneAlgorithm = new TaneAlgorithm();

        ODAlgorithm ODAlgorithm = new ODAlgorithm();


        ORDERLhsRhs ORDERAlgorithm = new ORDERLhsRhs();

        System.out.println("Algorithm: " + AlgorithmName);
        System.out.println("InterestingnessPrune: " + InterestingnessPrune);
        System.out.println("InterestingnessThreshold: " + InterestingnessThreshold);
        System.out.println("BidirectionalTrue: " + BidirectionalTrue);
        System.out.println("BidirectionalPruneTrue: " + BidirectionalPruneTrue);

        try {
            long startTime = System.currentTimeMillis();

            for(int i=0; i<RunTimeNumber; i ++) {

                if (AlgorithmName.equals("TANE"))
                    taneAlgorithm.execute();

                if (AlgorithmName.equals("FastOD"))
                    ODAlgorithm.execute();

                if (AlgorithmName.equals("ORDER"))
                    ORDERAlgorithm.execute();

                FirstTimeRun = false;
            }

            long endTime = System.currentTimeMillis();
            long runTime = (endTime - startTime)/RunTimeNumber;

            System.out.println("Run Time (ms): " + runTime);
        }catch(Exception ex){
            System.out.println("Error");
            ex.printStackTrace();
        }

        printTime();

        //print results to a file
        try {
            BufferedWriter bw =
                    new BufferedWriter(new FileWriter("D:/Code/Datasets/OD/unionN.txt"));

            for(String str : odList)
                bw.write(str + "\n");

            bw.close();
        }catch(Exception ex){

        }

    }

    public static void printTime(){
        Calendar now = Calendar.getInstance();
        int year = now.get(Calendar.YEAR);
        int month = now.get(Calendar.MONTH) + 1; // Note: zero based!
        int day = now.get(Calendar.DAY_OF_MONTH);
        int hour = now.get(Calendar.HOUR_OF_DAY);
        int minute = now.get(Calendar.MINUTE);
        int second = now.get(Calendar.SECOND);
        int millis = now.get(Calendar.MILLISECOND);

        System.out.printf("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        System.out.println("\n");
    }

    public static void printOpenBitSet(OpenBitSet bitSet, int maxLength){
        for(int i=0; i<maxLength; i ++){
            if(bitSet.get(i))
                System.out.print(1 + " ");
            else
                System.out.print(0 + " ");
        }
        System.out.println("");
    }

    public static void findUnion(){
        try {
            BufferedReader br = new BufferedReader(new FileReader("D:/Code/Datasets/OD/union.txt"));

            Set<String> union = new HashSet<String>();
            String str = null;
            while((str = br.readLine()) != null){
                union.add(str);
            }

            System.out.println("Union Size: " + union.size());

            br.close();
        }catch(Exception ex){

        }
    }
}

