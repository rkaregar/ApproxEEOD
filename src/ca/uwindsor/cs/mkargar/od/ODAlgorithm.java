package ca.uwindsor.cs.mkargar.od;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import org.apache.lucene.util.OpenBitSet;
//import sun.applet.Main;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.math.BigInteger;


/**
 * Created by Mehdi on 6/29/2016.
 */
public class ODAlgorithm {

    private Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level_minus1 = null;
    private Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level0 = null;
    private Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level1 = null;
    private Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>> prefix_blocks = null;

    private String tableName;
    private int numberAttributes;
    private long numberTuples;
    private List<String> columnNames;
    private ObjectArrayList<ColumnIdentifier> columnIdentifiers;

    private LongBigArrayBigList tTable;

    private List<String> columnNamesList;
    private List<List<String>> rowList;
    
    
    public int numOfAODLIS = 0, numofAODGreedy;
    public long timeAODLIS = 0, timeAODGreedy;
    public double improvementPercentage;


    //OD
    //for each attribute, in order, we have a list of its partition, sorted based on their values in ASC order
    ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>> TAU_SortedList;
    ArrayList<ObjectBigArrayBigList<Integer>> attributeValuesList;

    List<FDODScore> FDODScoreList;
    List<FDODScore> FDAODScoreList;
    Map<OpenBitSet, BigInteger> XScoreMap = new HashMap<OpenBitSet, BigInteger>();
    HashMap<Long, List<Long>> ViolationsTable = new HashMap<>();
    int answerCountFD = 1;
    int answerCountOD = 1;

    public void execute() throws AlgorithmExecutionException {



        FDODScoreList = new ArrayList<FDODScore>();
        FDAODScoreList = new ArrayList<FDODScore>();

        long start1 = System.currentTimeMillis();

        level0 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        level1 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        prefix_blocks = new Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>>();

        // Get information about table from database or csv file
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = loadData();
        setColumnIdentifiers();
        numberAttributes = this.columnNames.size();

        //Jarek
        double percent = (double)MainClass.violationThreshold/100.0;
        MainClass.violationThreshold = (int)(percent * numberTuples);

        System.out.println("violationThreshold:" + MainClass.violationThreshold);

        // Initialize table used for stripped partition product
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }

        //Begin Initialize Level 0
        CombinationHelper chLevel0 = new CombinationHelper();

        OpenBitSet rhsCandidatesLevel0 = new OpenBitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);

        ObjectArrayList<OpenBitSet> swapCandidatesLevel0 = new ObjectArrayList<OpenBitSet>();//the C_s is empty for L0
        chLevel0.setSwapCandidates(swapCandidatesLevel0);

        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
        chLevel0.setPartition(spLevel0);

        //spLevel0 = null;
        level0.put(new OpenBitSet(), chLevel0);
        //chLevel0 = null;
        //End Initialize Level 0

        //OD
        TAU_SortedList = new ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>>();
        attributeValuesList = new ArrayList<ObjectBigArrayBigList<Integer>>();

        //Begin Initialize Level 1
        for (int i = 1; i <= numberAttributes; i++) {
            OpenBitSet combinationLevel1 = new OpenBitSet();
            combinationLevel1.set(i);

            CombinationHelper chLevel1 = new CombinationHelper();
            OpenBitSet rhsCandidatesLevel1 = new OpenBitSet();
            rhsCandidatesLevel1.set(1, numberAttributes + 1);
            chLevel1.setRhsCandidates(rhsCandidatesLevel0);

            ObjectArrayList<OpenBitSet> swapCandidatesLevel1 = new ObjectArrayList<OpenBitSet>();//the C_s is empty for L1
            chLevel1.setSwapCandidates(swapCandidatesLevel1);

            //we also initialize TAU_SortedList with all equivalent classes, even for size 1
            StrippedPartition spLevel1 =
                    new StrippedPartition(partitions.get(i - 1), TAU_SortedList, attributeValuesList, numberTuples);
            chLevel1.setPartition(spLevel1);

            level1.put(combinationLevel1, chLevel1);
        }
        //End Initialize Level 1

        if(MainClass.FirstTimeRun) {
            //Set the results to return to the client

            //Print the results
            System.out.println("# ROW    : " + numberTuples);
            System.out.println("# COLUMN : " + numberAttributes);
            System.out.println("");
        }

        //partitions = null;

        long end1 = System.currentTimeMillis();
        //System.out.println("Time Before While Loop : " + (end1 - start1));

        int L = 1;
        while (!level1.isEmpty() && L <= numberAttributes) {
            //compute dependencies for a level

            //System.out.println("LEVEL : " + L + " size : " + level1.size() + " # FD : " + numberOfFD + " # OD : " + numberOfOD);

            computeODs(L);

            // prune the search space
            if(MainClass.Prune)
                prune(L);

            // compute the combinations for the next level
            generateNextLevel();
            L++;
        }

        //        System.out.println("Time Hash : " + TimeToPreparePIHash);
        //        System.out.println("Time TAU : " + TimeToPrepareTAU);
        //        System.out.println("Time AB Compare : " + TimeToCompareAB);
        //        System.out.println("");
        if(MainClass.FirstTimeRun) {
            //Set the results to return to the client


            //Print the results
            System.out.println("# FD : " + numberOfFD);
            System.out.println("# OD : " + numberOfOD);
            System.out.println("# AOD : " + numberOfAOD);
            System.out.println("");
        }

        //sore FDODScoreList

        if(FDODScoreList.size() < MainClass.topk)
            MainClass.topk = FDODScoreList.size();

        Collections.sort(FDODScoreList, FDODScore.FDODScoreComparator());

        if(MainClass.FirstTimeRun){
            int od_num = 1; //Keeps track of how many OCDs were discovered
            System.out.println("SORTED TOP-K  SORTED TOP-K  SORTED TOP-K  SORTED TOP-K");
            // for(int i=0; i<MainClass.topk; i ++){ // Sorted from lowest score to higheest
            for(int i = MainClass.topk - 1; i >= 0; i--){ //Sorted from the highest score to the lowest
                FDODScore fdodScore = FDODScoreList.get(i);
                System.out.print( "[" + od_num + "  SCORE: " + fdodScore.score + "] ");
                String ODFDString = "";
                if(fdodScore.functionalDependency != null){
                    ODFDString += "FD: ";
                    ODFDString += fdodScore.functionalDependency.toString(); //Set the FD
                    //Convert the FD into OD Canonical form as per the paper
                    ODFDString = ODFDString.replace("[","{");
                    ODFDString = ODFDString.replace("]"," }");
                    ODFDString = ODFDString.replace("->.",": [ ] |--> ");
                    ODFDString = ODFDString.replace(".","");
                    ODFDString = ODFDString.replace("\"","");
                    System.out.println(ODFDString); //Print the FD
                }else{
                    if (fdodScore.AOD){
                        ODFDString = printOpenBitSetNames("AOD: ", fdodScore.X_minus_AB, fdodScore.oneAB); //Set/Print the OD
                    }else{
                        ODFDString = printOpenBitSetNames("OCD: ", fdodScore.X_minus_AB, fdodScore.oneAB); //Set/Print the OD
                    }
                }
                //Finally add the FD/OD to the results that are sent back to the client: seperated with delimiters
                ODFDString = od_num++ + "$" + ODFDString + "$" + fdodScore.score;
                //ODFDString += " (score: " + fdodScore.score + ")"; //Add the score
            }

            //Combine the AODs and the ODFDs together into one
            System.out.println("--------END----------");
            
            
            
            // TODO I don't know where else to put this stuff
            System.out.println("----------Approx comaprisons------------");
            System.out.println("num: " + numOfAODLIS + " vs. " + numofAODGreedy + ", thus: " + ((numOfAODLIS * 100. / numofAODGreedy) - 100));
            System.out.println("time: " + timeAODLIS + " vs. " + timeAODGreedy + ", thus: " + (100 - (timeAODLIS * 100. / timeAODGreedy)));
            System.out.println("improvement: " + (improvementPercentage * 100 / numofAODGreedy));
            System.out.println("----------------------------------------");
        }
        return;
    }


    private long TimeToPreparePIHash = 0;
    private long TimeToPrepareTAU = 0;
    private long TimeToCompareAB = 0;


    private void generateNextLevel() {

        //OD
        level_minus1 = level0;

        level0 = level1;
        level1 = null;
        System.gc();

        Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> new_level = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();

        buildPrefixBlocks();

        for (ObjectArrayList<OpenBitSet> prefix_block_list : prefix_blocks.values()) {

            // continue only, if the prefix_block contains at least 2 elements
            if (prefix_block_list.size() < 2) {
                continue;
            }

            ObjectArrayList<OpenBitSet[]> combinations = getListCombinations(prefix_block_list);
            for (OpenBitSet[] c : combinations) {
                OpenBitSet X = (OpenBitSet) c[0].clone();
                X.or(c[1]);

                if (checkSubsets(X)) {
                    StrippedPartition st = null;
                    CombinationHelper ch = new CombinationHelper();
                    if (level0.get(c[0]).isValid() && level0.get(c[1]).isValid()) {
                        st = multiply(level0.get(c[0]).getPartition(), level0.get(c[1]).getPartition());
                    } else {
                        ch.setInvalid();
                    }
                    OpenBitSet rhsCandidates = new OpenBitSet();

                    ch.setPartition(st);
                    ch.setRhsCandidates(rhsCandidates);

                    new_level.put(X, ch);
                }
            }
        }

        level1 = new_level;
    }

    /**
     * Calculate the product of two stripped partitions and return the result as a new stripped partition.
     *
     * @param pt1: First StrippedPartition
     * @param pt2: Second StrippedPartition
     * @return A new StrippedPartition as the product of the two given StrippedPartitions.
     */
    public StrippedPartition multiply(StrippedPartition pt1, StrippedPartition pt2) {
        ObjectBigArrayBigList<LongBigArrayBigList> result = new ObjectBigArrayBigList<LongBigArrayBigList>();
        ObjectBigArrayBigList<LongBigArrayBigList> pt1List = pt1.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> pt2List = pt2.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> partition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        long noOfElements = 0;
        // iterate over first stripped partition and fill tTable.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, i);
            }
            partition.add(new LongBigArrayBigList());
        }
        // iterate over second stripped partition.
        for (long i = 0; i < pt2List.size64(); i++) {
            for (long t_id : pt2List.get(i)) {
                // tuple is also in an equivalence class of pt1
                if (tTable.get(t_id) != -1) {
                    partition.get(tTable.get(t_id)).add(t_id);
                }
            }
            for (long tId : pt2List.get(i)) {
                // if condition not in the paper;
                if (tTable.get(tId) != -1) {
                    if (partition.get(tTable.get(tId)).size64() > 1) {
                        LongBigArrayBigList eqClass = partition.get(tTable.get(tId));
                        result.add(eqClass);
                        noOfElements += eqClass.size64();
                    }
                    partition.set(tTable.get(tId), new LongBigArrayBigList());
                }
            }
        }
        // cleanup tTable to reuse it in the next multiplication.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, -1);
            }
        }
        return new StrippedPartition(result, noOfElements);
    }

    /**
     * Checks whether all subsets of X (with length of X - 1) are part of the last level.
     * Only if this check return true X is added to the new level.
     *
     * @param X
     * @return
     */
    private boolean checkSubsets(OpenBitSet X) {
        boolean xIsValid = true;

        // clone of X for usage in the following loop
        OpenBitSet Xclone = (OpenBitSet) X.clone();

        for (int l = X.nextSetBit(0); l >= 0; l = X.nextSetBit(l + 1)) {
            Xclone.clear(l);
            if (!level0.containsKey(Xclone)) {
                xIsValid = false;
                break;
            }
            Xclone.set(l);
        }

        return xIsValid;
    }

    /**
     * Get all combinations, which can be built out of the elements of a prefix block
     *
     * @param list: List of OpenBitSets, which are in the same prefix block.
     * @return All combinations of the OpenBitSets.
     */
    private ObjectArrayList<OpenBitSet[]> getListCombinations(ObjectArrayList<OpenBitSet> list) {
        ObjectArrayList<OpenBitSet[]> combinations = new ObjectArrayList<OpenBitSet[]>();
        for (int a = 0; a < list.size(); a++) {
            for (int b = a + 1; b < list.size(); b++) {
                OpenBitSet[] combi = new OpenBitSet[2];
                combi[0] = list.get(a);
                combi[1] = list.get(b);
                combinations.add(combi);
            }
        }
        return combinations;
    }

    /**
     * Build the prefix blocks for a level. It is a HashMap containing the
     * prefix as a key and the corresponding attributes as  the value.
     */
    private void buildPrefixBlocks() {
        this.prefix_blocks.clear();
        for (OpenBitSet level_iter : level0.keySet()) {
            OpenBitSet prefix = getPrefix(level_iter);

            if (prefix_blocks.containsKey(prefix)) {
                prefix_blocks.get(prefix).add(level_iter);
            } else {
                ObjectArrayList<OpenBitSet> list = new ObjectArrayList<OpenBitSet>();
                list.add(level_iter);
                prefix_blocks.put(prefix, list);
            }
        }
    }

    /**
     * Get prefix of OpenBitSet by copying it and removing the last Bit.
     *
     * @param bitset
     * @return A new OpenBitSet, where the last set Bit is cleared.
     */
    private OpenBitSet getPrefix(OpenBitSet bitset) {
        OpenBitSet prefix = (OpenBitSet) bitset.clone();
        prefix.clear(getLastSetBitIndex(prefix));
        return prefix;
    }

    private long getLastSetBitIndex(OpenBitSet bitset) {
        int lastSetBit = 0;
        for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
            lastSetBit = A;
        }
        return lastSetBit;
    }

    /**
     * Prune the current level (level1) by removing all elements with no rhs candidates.
     * All keys are marked as invalid.
     * In case a key is found, minimal dependencies are added to the result receiver.
     *
     * @throws AlgorithmExecutionException if the result receiver cannot handle the functional dependency.
     */
    private void prune(int L) throws AlgorithmExecutionException {



        if(L >= 2) {

            ObjectArrayList<OpenBitSet> elementsToRemove = new ObjectArrayList<OpenBitSet>();
            for (OpenBitSet x : level1.keySet()) {

                if ( level1.get(x).getRhsCandidates().isEmpty() && (level1.get(x).getSwapCandidates().isEmpty()) ) {
                    elementsToRemove.add(x);
                    //this continue is useful when we add KEY checking after this if statement
                    continue;
                }

            }

            for (OpenBitSet x : elementsToRemove) {
                level1.remove(x);
            }
        }
    }


    /**
     * Computes the dependencies and ODs for the current level (level1).
     *
     * @throws AlgorithmExecutionException
     */
    private void computeODs(int L) throws AlgorithmExecutionException {


        initializeCplus_c_ForLevel(); //Line 2 in Algorithm 3

        //OD
        initializeCplus_s_ForLevel(L);

        // iterate through the combinations of the level
        for (OpenBitSet X : level1.keySet()) {

            //OD remove check for isValid for now
            //if (level1.get(X).isValid()) {

            //*************************** FUNCTIONAL DEPENDENCIES (CANONICAL FORM 1)

            // Build the intersection between X and C_plus(X)
            OpenBitSet C_plus = level1.get(X).getRhsCandidates();
            OpenBitSet intersection = (OpenBitSet) X.clone();
            intersection.intersect(C_plus);

            // clone of X for usage in the following loop
            OpenBitSet Xclone = (OpenBitSet) X.clone();

            // iterate through all elements (A) of the intersection
            for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)) {
                Xclone.clear(A);

                // check if X\A -> A is valid
                StrippedPartition spXwithoutA = level0.get(Xclone).getPartition();
                StrippedPartition spX = level1.get(X).getPartition();

                if (spX.getError() == spXwithoutA.getError()) {

                    //we found one FD here

                    OpenBitSet XwithoutA = (OpenBitSet) Xclone.clone();

                    processFunctionalDependency(XwithoutA, A, spXwithoutA, L);

                    // remove A from C_plus(X)
                    if(MainClass.Prune)
                        level1.get(X).getRhsCandidates().clear(A);

                    // remove all B in R\X from C_plus(X)
                    if(MainClass.Prune) {
                        OpenBitSet RwithoutX = new OpenBitSet();

                        // set to R
                        RwithoutX.set(1, numberAttributes + 1);
                        // remove X
                        RwithoutX.andNot(X);

                        for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                            level1.get(X).getRhsCandidates().clear(i);
                        }
                    }

                }
                Xclone.set(A);
            }



            //*************************** ORDER DEPENDENCIES (CANONICAL FORM 2)
            ArrayList<OpenBitSet> removeFromC_s_List = new ArrayList<OpenBitSet>();

            for(OpenBitSet oneAB : level1.get(X).getSwapCandidates()){

                //printOpenBitSet("Line 17  and X is : " , X);
                //printOpenBitSet("Line 17  and AB is : " , oneAB);

                //                if (L == 3 && X.equals(XTestValue) && oneAB.equals(ABTestValue)) {
                //                //if (L == 3 && X.equals(XTestValue)) {
                //                    int aaaaaa = 0;
                //                }

                //readon so far: A = B;     B = C
                // C_c_plus(X/A)_100K = 16 , C_c_plus(X/A)_170K = 30, C_c_plus(CD)
                // C_c_plus(X/B)_100K = 16 , C_c_plus(X/B)_170K = 30, C_c_plus(BD)

                //line 18, Algorithm 3
                OpenBitSet[] A_B_Separate = getSeparateOpenBitSet_AB(oneAB);
                int[] A_B_Index = getIndexOfOpenBitSet_AB(oneAB);

                OpenBitSet A = A_B_Separate[0];
                OpenBitSet B = A_B_Separate[1];

                int A_index = A_B_Index[0]; //starts from 1
                int B_index = A_B_Index[1]; //starts from 1

                OpenBitSet X_minus_A = (OpenBitSet) X.clone();
                X_minus_A.remove(A);
                OpenBitSet C_c_X_minus_A = level0.get(X_minus_A).getRhsCandidates();
                OpenBitSet C_c_X_minus_A_Clone = (OpenBitSet) C_c_X_minus_A.clone();
                C_c_X_minus_A_Clone.union(B);

                OpenBitSet X_minus_B = (OpenBitSet) X.clone();
                X_minus_B.remove(B);
                OpenBitSet C_c_X_minus_B = level0.get(X_minus_B).getRhsCandidates();
                OpenBitSet C_c_X_minus_B_Clone = (OpenBitSet) C_c_X_minus_B.clone();
                C_c_X_minus_B_Clone.union(A);

                //this is exactly the if statement in line 18
                if(  !(C_c_X_minus_B.equals(C_c_X_minus_B_Clone)) ||  !(C_c_X_minus_A.equals(C_c_X_minus_A_Clone))){
                    removeFromC_s_List.add(oneAB);


                }else{

                    long L1 = System.currentTimeMillis();

                    ViolationsTable.clear();

                    //line 20, if( X\{A,B} : A ~ B)

                    //step 1: find X\{A,B}
                    OpenBitSet X_minus_AB = (OpenBitSet) X.clone();
                    X_minus_AB.remove(A);
                    X_minus_AB.remove(B);

                    ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition_X_minus_AB =
                            level_minus1.get(X_minus_AB).getPartition().getStrippedPartition();

                    //create hash table based on strippedPartition_X_minus_AB
                    Object2ObjectOpenHashMap<Long, Integer> strippedPartition_X_minus_AB_Hash =
                            new Object2ObjectOpenHashMap<Long, Integer>();
                    int counter = 0;
                    for(LongBigArrayBigList strippedPartitionElement : strippedPartition_X_minus_AB){
                        for(long element_index : strippedPartitionElement){
                            strippedPartition_X_minus_AB_Hash.put(element_index, counter);
                        }
                        counter ++;
                    }

                    long L2 = System.currentTimeMillis();
                    TimeToPreparePIHash += (L2 - L1);

                    long L3 = System.currentTimeMillis();

                    ObjectBigArrayBigList<LongBigArrayBigList> sorted_TAU_A = TAU_SortedList.get(A_index - 1);//A_index starts from 1

                    ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>> PI_X_TAU_A =
                            new ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>>();

                    //PI_X_TAU_A is Table 6 in my Excel file
                    //the number of items in this list is equal to the number of items in strippedPartition_X_minus_AB
                    for(LongBigArrayBigList strippedPartitionElement : strippedPartition_X_minus_AB){
                        PI_X_TAU_A.add(new ObjectBigArrayBigList<LongBigArrayBigList>());
                    }

                    for(LongBigArrayBigList tau_A_element : sorted_TAU_A){

                        Set<Integer> seenIndexSet = new HashSet<Integer>();
                        for(long l_a : tau_A_element){
                            //insert in PI_X_TAU_A
                            if(strippedPartition_X_minus_AB_Hash.containsKey(l_a)) {

                                int index_in_PI_X_TAU_A = strippedPartition_X_minus_AB_Hash.get(l_a);
                                if (seenIndexSet.contains(index_in_PI_X_TAU_A)) {
                                    //In this case, this will be added to the last list
                                } else {
                                    //In this case, a new list is created
                                    seenIndexSet.add(index_in_PI_X_TAU_A);
                                    PI_X_TAU_A.get(index_in_PI_X_TAU_A).add(new LongBigArrayBigList());
                                }

                                long currentSize = PI_X_TAU_A.get(index_in_PI_X_TAU_A).size64();
                                PI_X_TAU_A.get(index_in_PI_X_TAU_A).get(currentSize - 1).add(l_a);

                            }
                        }
                    }

                    long L4 = System.currentTimeMillis();
                    TimeToPrepareTAU += (L4 - L3);

                    //check to see whether a swap happened or not

                    long L5 = System.currentTimeMillis();

                    boolean swapHappen = false;
                    int numSwaps = 0;
                    Long violationToRemove;
                    int violationIterations = 0;
                    long maxB_index;
                    long minB_index;

                    ObjectBigArrayBigList<Integer> bValues = attributeValuesList.get(B_index - 1);
                    boolean violationCriteria = true; //original
                    // boolean violationCriteria = false;  //test
                    List<Long> toRemove = new ArrayList<>();
                    // if(MainClass.approxODBoolean){violationCriteria = true;} //original

                    //                    for(int j=0; j<PI_X_TAU_A.size64() && (!swapHappen); j ++){
                    //                      for(int j=0; j<PI_X_TAU_A.size64() && (numSwaps < MainClass.swapThreshold); j ++){
                    //if(MainClass.approxODBoolean){ //todel
    
                    
                    swapHappen = CandidateSwap.hasSwap(PI_X_TAU_A, bValues);
                    
                    int numRemovalLIS = -2, numRemovalGreedy = -2;
                    
                    if (swapHappen && (MainClass.approxAlgo.equals("LIS") || MainClass.approxAlgo.equals("Both"))) {
                        long tAOD1 = System.nanoTime();
                        
                        numRemovalLIS = CandidateLIS.computeLIS(PI_X_TAU_A, bValues);
                        
//                        if (numRemovalLIS <= MainClass.violationThreshold) {
//                            numOfAOD1 += 1;
//                        }
//                        System.out.println("----- actual ratio:" + (((double)numRemovalLIS) / numberTuples));
                        
                        timeAODLIS += (System.nanoTime() - tAOD1);
                    }
                    
                    if (swapHappen && (MainClass.approxAlgo.equals("Greedy") || MainClass.approxAlgo.equals("Both"))) {
                        long tAOD2 = System.nanoTime();
                        
                        CandidateGreedyApprox cga = new CandidateGreedyApprox(PI_X_TAU_A, bValues);
                        numRemovalGreedy = cga.getRepairNum(MainClass.violationThreshold);
                        
//                        System.out.println("----- greedy ratio:" + (((double)numViol) / numberTuples));
                        timeAODGreedy += (System.nanoTime() - tAOD2);
                    }
    
                    if (!swapHappen) {  // An exact valid OD
                        //FINDING SCORE OF ONE ANSWER
                        BigInteger score = BigInteger.valueOf(0);
                        if (!XScoreMap.containsKey(X_minus_AB)) {
                            score = calculateInterestingnessScore(strippedPartition_X_minus_AB, X_minus_AB);
                            XScoreMap.put(X_minus_AB, score);
                        } else {
                            score = XScoreMap.get(X_minus_AB);
                        }
                        FDODScore fdodScore = new FDODScore(score, X_minus_AB, oneAB);
                        FDODScoreList.add(fdodScore);
                        //calculate interestingness score
                        numberOfOD++;
                        removeFromC_s_List.add(oneAB);
                    } else {
                        double violationRatioLIS = -1, violationRatioGreedy = -1;
                        if (numRemovalLIS != -2 && numRemovalLIS < MainClass.violationThreshold && MainClass.approxODBoolean) {
//                        String AODString = printOpenBitSetNames("Approx OCD, LIS: ", X_minus_AB, oneAB);
//                        BigInteger score = BigInteger.valueOf(0);
//                        if(!XScoreMap.containsKey(X_minus_AB)){
//                            score = calculateInterestingnessScore(strippedPartition_X_minus_AB, X_minus_AB);
//                            XScoreMap.put(X_minus_AB, score);
//                        }else{
//                            score = XScoreMap.get(X_minus_AB);
//                        }
//                        FDODScore fdodScore = new FDODScore(score, X_minus_AB, oneAB, true);
                            numOfAODLIS++;
                            violationRatioLIS = (double) numRemovalLIS/numberTuples;
                            System.out.println("***** violationRatio LIS:" + violationRatioLIS);
                        }
                        if (numRemovalGreedy > 0 && numRemovalGreedy < MainClass.violationThreshold && MainClass.approxODBoolean) {
                            numofAODGreedy++;
                            violationRatioGreedy = (double) numRemovalGreedy/numberTuples;
                            System.out.println("***** violationRatio Greedy:" + violationRatioGreedy);
                        }
                        
                        if (MainClass.approxAlgo.equals("Both") && numRemovalGreedy > 0) {
                            improvementPercentage += (violationRatioGreedy - violationRatioLIS) / violationRatioGreedy;
                            if ((violationRatioGreedy - violationRatioLIS) / violationRatioGreedy > 0)
                                System.out.println("yo look at this " + ((violationRatioGreedy - violationRatioLIS) / violationRatioGreedy * 100));
                        }
                    }
                }
            }

            //remove ABs
            if(MainClass.Prune) {
                for (OpenBitSet removedAB : removeFromC_s_List) {
                    level1.get(X).getSwapCandidates().remove(removedAB);
                }
            }
            //}
        }

    }

    private BigInteger calculateInterestingnessScore(
            ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition,
            OpenBitSet X){
        BigInteger score = BigInteger.valueOf(0);

        //System.out.println("MainClass.MaxRowNumber = " + MainClass.MaxRowNumber);

        if(X.isEmpty()){
            score = BigInteger.valueOf(MainClass.MaxRowNumber).multiply(BigInteger.valueOf(MainClass.MaxRowNumber));
        }else {

            int totalNumberOfRowsCountedAlready = 0; //this is used to add stirppted partition rows later
            for (LongBigArrayBigList strippedPartitionElement : strippedPartition) {
                //score += (strippedPartitionElement.size64() * strippedPartitionElement.size64());
                score = score.add(BigInteger.valueOf((strippedPartitionElement.size64() * strippedPartitionElement.size64())));
                totalNumberOfRowsCountedAlready += strippedPartitionElement.size64();
            }
            //add the stripped partitions, since each of them is 1, raising them to the power of two will not change their value
            score = score.add(BigInteger.valueOf(numberTuples - totalNumberOfRowsCountedAlready));
            //score += (numberTuples - totalNumberOfRowsCountedAlready);
        }

        return score;
    }

    public int numberOfOD = 0;
    private int numberOfAOD = 0;

    private OpenBitSet[] getSeparateOpenBitSet_AB(OpenBitSet oneAB){

        OpenBitSet A = new OpenBitSet();
        OpenBitSet B = new OpenBitSet();

        boolean foundA = false;

        for(int i=0; i<numberAttributes+1; i ++){
            if(oneAB.get(i)){
                if(!foundA){
                    foundA = true;
                    A.set(i);
                }else{
                    B.set(i);
                }
            }
        }

        OpenBitSet[] results = new OpenBitSet[2];
        results[0] = A;
        results[1] = B;

        return results;
    }

    private int[] getIndexOfOpenBitSet_AB(OpenBitSet oneAB){

        int A_index = -1;
        int B_index = -1;

        boolean foundA = false;

        for(int i=0; i<numberAttributes+1; i ++){
            if(oneAB.get(i)){
                if(!foundA){
                    foundA = true;
                    A_index = i;
                }else{
                    B_index = i;
                }
            }
        }

        int[] results = new int[2];
        results[0] = A_index;
        results[1] = B_index;

        return results;
    }

    /**
     * Adds the FD lhs -> a to the resultReceiver and also prints the dependency.
     *
     * @param lhs: left-hand-side of the functional dependency
     * @param a:   dependent attribute. Possible values: 1 <= a <= maxAttributeNumber.
     */
    private void processFunctionalDependency(OpenBitSet lhs, int a, StrippedPartition spXwithoutA, int L) {
        try {
            addDependencyToResultReceiver(lhs, a, spXwithoutA, L);
        } catch (ColumnNameMismatchException e) {
            e.printStackTrace();
        }
    }

    private int numberOfFD = 0;

    private void addDependencyToResultReceiver(OpenBitSet X, int a, StrippedPartition spXwithoutA, int L) throws ColumnNameMismatchException {

        ColumnIdentifier[] columns = new ColumnIdentifier[(int) X.cardinality()];
        int j = 0;
        for (int i = X.nextSetBit(0); i >= 0; i = X.nextSetBit(i + 1)) {
            columns[j++] = this.columnIdentifiers.get(i - 1);
        }
        ColumnCombination colCombination = new ColumnCombination(columns);
        FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get((int) a - 1));

        //FINDING SCORE OF ONE ANSWER
        BigInteger score = BigInteger.valueOf(0);
        if(!XScoreMap.containsKey(X)){
            score = score.add(calculateInterestingnessScore(spXwithoutA.getStrippedPartition(), X));
            XScoreMap.put(X, score);
        }else{
            //score = XScoreMap.get(X);
            score = score.add(XScoreMap.get(X));
        }

        FDODScore fdodScore = new FDODScore(score, fdResult);
        FDODScoreList.add(fdodScore);

        numberOfFD ++;
        //        System.out.println("##### FD Found " + numberOfFD);
        //System.out.println("FD#  " + (answerCountFD++) + "  #SCORE#  " + score + "  #L#  " + L );

        //System.out.println(score + "#" +numberOfFD+ "#L#" + L + "#FD:#" + fdResult);

        //System.out.println(score);
        //        System.out.println("");
    }

    //OD
    private void initializeCplus_s_ForLevel(int L) {

        if(L == 2){ //Line 3 in Algorithm 3

            for (OpenBitSet X : level1.keySet()) {
                OpenBitSet Xclone = (OpenBitSet) X.clone();

                ObjectArrayList<OpenBitSet> SofX = new ObjectArrayList<OpenBitSet>();
                SofX.add(Xclone);//at level 2, each element X has one C_s

                CombinationHelper ch = level1.get(X);
                ch.setSwapCandidates(SofX);

                //                printOpenBitSet("new C+s size is " + SofX.size() + " , and X is : " , X);
                //                for(OpenBitSet openAB : SofX){
                //                    printOpenBitSet("pair AB: ", openAB);
                //                }
            }

        }else{
            if(L > 2){ //Line 5 in Algorithm 3

                //loop through all members of current level, this loop is missing in the pseudo-code
                for (OpenBitSet X : level1.keySet()) {

                    ObjectArrayList<OpenBitSet> allPotentialSwapCandidates = new ObjectArrayList<OpenBitSet>();

                    //clone of X for usage in the following loop
                    //loop over all X\C (line 6 Algorithm 3)
                    OpenBitSet Xclone1 = (OpenBitSet) X.clone();
                    for (int C = X.nextSetBit(0); C >= 0; C = X.nextSetBit(C + 1)) {
                        Xclone1.clear(C);
                        //now Xclone is X/C
                        ObjectArrayList<OpenBitSet> C_s_withoutC_List = level0.get(Xclone1).getSwapCandidates();
                        for(OpenBitSet oneAB : C_s_withoutC_List){
                            if(!allPotentialSwapCandidates.contains(oneAB)){
                                allPotentialSwapCandidates.add(oneAB);
                            }
                        }
                        Xclone1.set(C);
                    }

                    ObjectArrayList<OpenBitSet> allActualSwapCandidates = new ObjectArrayList<OpenBitSet>();

                    //loop over all potential {A,B}
                    for(OpenBitSet oneAB : allPotentialSwapCandidates){
                        //step 1: form X\{A, B}
                        OpenBitSet X_minus_AB = (OpenBitSet) X.clone();
                        X_minus_AB.remove(oneAB);//This is X\{A,B}

                        //now we have to examine all members of X\{A,B}
                        OpenBitSet Xclone2 = (OpenBitSet) X.clone();

                        boolean doesAllofThemContains_AB = true;

                        //loop over X_minus_AB, but check c_s_plus on X_minus_D
                        for (int D = X_minus_AB.nextSetBit(0); D >= 0; D = X_minus_AB.nextSetBit(D + 1)) {
                            Xclone2.clear(D);
                            //now Xclone2 does not contain D
                            ObjectArrayList<OpenBitSet> C_s_withoutD_List = level0.get(Xclone2).getSwapCandidates();
                            if(!C_s_withoutD_List.contains(oneAB))
                                doesAllofThemContains_AB = false;

                            Xclone2.set(D);
                        }

                        if(doesAllofThemContains_AB) {


                            OpenBitSet X__clone_minusAB = (OpenBitSet) X.clone();
                            X__clone_minusAB.remove(oneAB);//This is X\{A,B}
                            ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition_X_minus_AB =
                                    level_minus1.get(X__clone_minusAB).getPartition().getStrippedPartition();

                            BigInteger score = BigInteger.valueOf(0);
                            if(!XScoreMap.containsKey(X__clone_minusAB)){
                                score = calculateInterestingnessScore(strippedPartition_X_minus_AB, X__clone_minusAB);
                                XScoreMap.put(X__clone_minusAB, score);
                            }else{
                                score = XScoreMap.get(X__clone_minusAB);
                            }

                            if(MainClass.InterestingnessPrune){
                                //check to see whether we should add oneAB or not
                                // System.out.println("score: "+ score);
                                // System.out.println("BigInteger.valueOf(MainClass.InterestingnessThreshold): "+ BigInteger.valueOf(MainClass.InterestingnessThreshold));
                                // System.out.println("score.compareTo(BigInteger.valueOf(MainClass.InterestingnessThreshold)): " + score.compareTo(BigInteger.valueOf(MainClass.InterestingnessThreshold)));
                                //if(score > MainClass.InterestingnessThreshold){
                                if(score.compareTo(BigInteger.valueOf(MainClass.InterestingnessThreshold)) == 1){
                                    allActualSwapCandidates.add(oneAB);
                                }

                            }else{
                                allActualSwapCandidates.add(oneAB);
                            }

                        }

                    }

                    CombinationHelper ch = level1.get(X);

                    //                    printOpenBitSet("new C+s size is " + allActualSwapCandidates.size() + " , and X is : " , X);
                    //                    for(OpenBitSet openAB : allActualSwapCandidates){
                    //                        printOpenBitSet("pair AB: ", openAB);
                    //                    }

                    ch.setSwapCandidates(allActualSwapCandidates);

                }
            }
        }
    }

    /**
     * Initialize Cplus_c (resp. rhsCandidates) for each combination of the level.
     */
    private void initializeCplus_c_ForLevel() {
        for (OpenBitSet X : level1.keySet()) {

            ObjectArrayList<OpenBitSet> CxwithoutA_list = new ObjectArrayList<OpenBitSet>();

            // clone of X for usage in the following loop
            OpenBitSet Xclone = (OpenBitSet) X.clone();
            for (int A = X.nextSetBit(0); A >= 0; A = X.nextSetBit(A + 1)) {
                Xclone.clear(A);
                OpenBitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();

                CxwithoutA_list.add(CxwithoutA);
                Xclone.set(A);
            }

            OpenBitSet CforX = new OpenBitSet();

            if (!CxwithoutA_list.isEmpty()) {
                CforX.set(1, numberAttributes + 1);
                for (OpenBitSet CxwithoutA : CxwithoutA_list) {

                    CforX.and(CxwithoutA);

                }
            }

            CombinationHelper ch = level1.get(X);


            OpenBitSet CforX_prune = new OpenBitSet();


            //printOpenBitSet("X: ", X);
            //printOpenBitSet("Cc+: ", CforX);

            boolean isRemovedFromCPlus = false;

            for(int i=1; i<numberAttributes+1; i ++){
                if(CforX.get(i)){

                    if(X.get(i)) {
                        //we have to check the score of X\i
                        OpenBitSet X__clone = (OpenBitSet) X.clone();
                        X__clone.clear(i);
                        //now X__clone is X\A
                        //printOpenBitSet("X\\A: ", X__clone);

                        if(X__clone.isEmpty()){
                            CforX_prune.set(i);
                        }else{
                            //add to the map to improve performance: XScoreMap.containsKey(CforX_clone)

                            StrippedPartition spXwithoutA = level0.get(X__clone).getPartition();

                            BigInteger score = BigInteger.valueOf(0);
                            if(!XScoreMap.containsKey(X__clone)){
                                score = calculateInterestingnessScore(spXwithoutA.getStrippedPartition(), X__clone);
                                XScoreMap.put(X__clone, score);
                            }else{
                                score = XScoreMap.get(X__clone);
                            }
                            //if (score > MainClass.InterestingnessThreshold) {
                            if (score.compareTo(BigInteger.valueOf(MainClass.InterestingnessThreshold)) == 1) {
                                CforX_prune.set(i);
                            }else{
                                isRemovedFromCPlus = true;
                            }
                        }
                    }else{
                        //if it is not in X, it should stay in C_c+
                        CforX_prune.set(i);
                    }

                }

            }

            if(MainClass.InterestingnessPrune){


                if(isRemovedFromCPlus){
                    for(int i=1; i<numberAttributes+1; i ++) {
                        if (CforX_prune.get(i)) {
                            if(X.get(i)) {
                                //do nothing
                            }else{
                                CforX_prune.clear(i);
                            }
                        }
                    }
                }

                //printOpenBitSet("new C+c, and X is : ", X);
                //printOpenBitSet("C+c prune: ", CforX_prune);

                ch.setRhsCandidates(CforX_prune);
            }else{
                ch.setRhsCandidates(CforX);
            }



        }
    }

    private void setColumnIdentifiers() {
        this.columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(this.columnNames.size());
        for (String column_name : this.columnNames) {
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, column_name));
        }
    }

    private void fillData() {

        String csvFile = MainClass.DatasetFileName;
        BufferedReader br = null;
        String line = "";

        try {
            br = new BufferedReader(new FileReader(csvFile));

            line = br.readLine();
            String[] attributes = line.split(MainClass.cvsSplitBy);

            columnNamesList = new ArrayList<String>();

            long columnCount = 0;
            for(String attributeName : attributes){
                if(columnCount < MainClass.MaxColumnNumber) {
                    columnNamesList.add(attributeName);
                    columnCount ++;
                }
            }

            rowList = new ArrayList<List<String>>();

            long rowCount = 0;

            while ( ((line = br.readLine()) != null) && (rowCount < MainClass.MaxRowNumber)) {

                String[] tuples = line.split(MainClass.cvsSplitBy);

                List<String> row = new ArrayList<String>();

                long columnCountForThisRow = 0;
                for(String tupleValue : tuples){
                    if(columnCountForThisRow < MainClass.MaxColumnNumber) {
                        row.add(tupleValue);
                        columnCountForThisRow++;
                    }
                }
                rowList.add(row);

                rowCount ++;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> loadData() {

        fillData();

        this.numberAttributes = columnNamesList.size();//input.numberOfColumns();
        this.tableName = "";//input.relationName();

        this.columnNames = columnNamesList;// input.columnNames();

        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions =
                new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(this.numberAttributes);
        for (int i = 0; i < this.numberAttributes; i++) {
            Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = new Object2ObjectOpenHashMap<Object, LongBigArrayBigList>();
            partitions.add(partition);
        }
        long tupleId = 0;

        //while (input.hasNext()) {
        for(int rowId=0; rowId<rowList.size(); rowId ++) {
            //List<String> row = input.next();
            List<String> row = rowList.get(rowId);

            for (int i = 0; i < this.numberAttributes; i++) {
                Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = partitions.get(i);
                String entry = row.get(i);
                if (partition.containsKey(entry)) {
                    partition.get(entry).add(tupleId);
                } else {
                    LongBigArrayBigList newEqClass = new LongBigArrayBigList();
                    newEqClass.add(tupleId);
                    partition.put(entry, newEqClass);
                }
            }

            tupleId++;
        }


        this.numberTuples = tupleId;
        return partitions;

    }

    private void printOpenBitSet(String message, OpenBitSet bitSet){
        System.out.print(message + "  ");
        for(int i=1; i<numberAttributes+1; i ++){
            if(bitSet.get(i))
                System.out.print(1 + " ");
            else
                System.out.print(0 + " ");
        }
        System.out.println("");
    }

    private String printOpenBitSetNames(String message, OpenBitSet bitSet, OpenBitSet bitSet2){
        String resultStr = "";
        resultStr += message;

        resultStr += "{ ";
        for(int i=1; i<numberAttributes+1; i ++){
            if(bitSet.get(i))
                resultStr += this.columnNames.get(i-1) + " ";
        }
        resultStr += "} : ";

        Boolean first = true; //Keeps track of the first attribute in a swap (eg. A ~ B, will be true for A and false for B)
        for(int i=1; i<numberAttributes+1; i ++){
            if(bitSet2.get(i)){
                resultStr += this.columnNames.get(i-1);
                if(first){resultStr += " ~ "; first=false;} //Add ~ for the first attriute in the swap
            }
        }
        resultStr = resultStr.replace("\"","");
        System.out.println(resultStr);
        return resultStr;
    }

    public static void printMap(Map mp) {
        Iterator it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
            it.remove(); // avoids a ConcurrentModificationException
        }
    }

    private void ViolationsTableAdd(long key, long newValue) {
        List<Long> currentValue = ViolationsTable.get(key);
        if (currentValue == null) {
            currentValue = new ArrayList<>();
            ViolationsTable.put(key, currentValue);
        }
        currentValue.add(newValue);
    }
}

class FDODScore{
    public BigInteger score;
    public OpenBitSet X_minus_AB;
    public OpenBitSet oneAB;
    public FunctionalDependency functionalDependency;
    public boolean AOD;

    public FDODScore(BigInteger score, OpenBitSet X_minus_AB, OpenBitSet oneAB){
        this.score = score;
        this.X_minus_AB = X_minus_AB;
        this.oneAB = oneAB;
        this.functionalDependency = null;
        this.AOD = false;
    }

    public FDODScore(BigInteger score, OpenBitSet X_minus_AB, OpenBitSet oneAB, boolean AOD){
        this.score = score;
        this.X_minus_AB = X_minus_AB;
        this.oneAB = oneAB;
        this.functionalDependency = null;
        this.AOD = AOD;
    }

    public FDODScore(BigInteger score, FunctionalDependency functionalDependency){
        this.score = score;
        this.X_minus_AB = null;
        this.oneAB = null;
        this.functionalDependency = functionalDependency;
    }

    public static Comparator<FDODScore> FDODScoreComparator(){

        Comparator comp = new Comparator<FDODScore>(){
            public int compare(FDODScore s1, FDODScore s2){
                return s1.score.compareTo(s2.score);
            }
        };
        return comp;
    }
}
