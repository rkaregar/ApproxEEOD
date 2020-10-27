package ca.uwindsor.cs.mkargar.od;

import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// given the values of a candidate, determines the LIS of it
public class CandidateLIS {
    
    public static int computeLIS(ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>> PI_X_TAU_A,
                                 ObjectBigArrayBigList<Integer> bValues) {
        int lis_cost = 0, lis = 0;
        for (int j = 0; j < PI_X_TAU_A.size64(); j++) {
            ObjectBigArrayBigList oneListInX = PI_X_TAU_A.get(j);
            
            List<Integer> allBs = new ArrayList<>();
            for (int i = 0; i < oneListInX.size64(); i++) {
                LongBigArrayBigList List1 = (LongBigArrayBigList) oneListInX.get(i);
                
                List<Integer> bs = new ArrayList<>();
                for (long index1 : List1) {
                    int value = bValues.get(index1);
                    bs.add(value);
                }
                Collections.sort(bs);
                allBs.addAll(bs);
            }
            
            int tmp = LongestIncreasingSubsequenceLength(allBs);
            lis_cost += (allBs.size() - tmp);  // this is the # of tuples that have to be deleted
            lis += tmp;
        }
        return lis_cost;
//        return lis;
    }
    
    // Binary search (note boundaries in the caller)
    // A[] is ceilIndex in the caller
    static int CeilIndex(int[] A, int l, int r, int key) {
        while (r - l > 1) {
            int m = l + (r - l) / 2;
            if (A[m] > key)
                r = m;
            else
                l = m;
        }
        
        return r;
    }
    
    static int LongestIncreasingSubsequenceLength(List<Integer> A) {
        int size = A.size();
        // Add boundary case, when array size is one
        if (size <= 1)
            return size;
        
        int[] tailTable = new int[size];
        int len; // always points empty slot
        
        tailTable[0] = A.get(0);
        len = 1;
        for (int i = 1; i < size; i++) {
            if (A.get(i) < tailTable[0])
                // new smallest value
                tailTable[0] = A.get(i);
            
            else if (A.get(i) >= tailTable[len - 1])
                // A[i] wants to extend largest subsequence
                tailTable[len++] = A.get(i);
            
            else
                // A[i] wants to be current end candidate of an existing
                // subsequence. It will replace ceil value in tailTable
                tailTable[CeilIndex(tailTable, -1, len - 1, A.get(i))] = A.get(i);
        }
        
        return len;
    }
}
