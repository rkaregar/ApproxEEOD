package ca.uwindsor.cs.mkargar.od;

import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

// Checks to see if the candidate has any swaps
public class CandidateSwap {
    public static boolean hasSwap(ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>> PI_X_TAU_A,
                                  ObjectBigArrayBigList<Integer> bValues) {  // return true if there are any swaps
        for (int j = 0; j < PI_X_TAU_A.size64(); j++) {
            ObjectBigArrayBigList oneListInX = PI_X_TAU_A.get(j);
            for (int i = 0; i < oneListInX.size64() - 1; i++) {
                LongBigArrayBigList List1 = (LongBigArrayBigList) oneListInX.get(i);
                LongBigArrayBigList List2 = (LongBigArrayBigList) oneListInX.get(i + 1);
                
                // Check to make sure a swap does not happen between List1 and List2 with respect to A and B
                int maxB_inList1 = -1;
                for (long index1 : List1) {
                    int value = bValues.get(index1);
                    if (value > maxB_inList1) {
                        maxB_inList1 = value;
                    }
                }
                
                int minB_inList2 = Integer.MAX_VALUE;
                for (long index2 : List2) {
                    int value = bValues.get(index2);
                    if (value < minB_inList2) {
                        minB_inList2 = value;
                    }
                }
                
                // NO Swap: maxB_inList1 < minB_inList2
                // Swap: maxB_inList1 > minB_inList2
                if (maxB_inList1 > minB_inList2) {
                    return true;
                }
            }
        }
        return false;
    }
}
