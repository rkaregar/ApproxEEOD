package ca.uwindsor.cs.mkargar.od;

import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

import java.util.*;

public class CandidateGreedyApprox {
    private ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>> PI_X_TAU_A;
    private ObjectBigArrayBigList<Integer> bValues;
    private HashMap<Long, Integer> tupId2APGNum;
    public static Node tailNode;  // This may not be the cleanest design possible...
    
    public CandidateGreedyApprox(ObjectBigArrayBigList<ObjectBigArrayBigList<LongBigArrayBigList>> PI_X_TAU_A,
                                 ObjectBigArrayBigList<Integer> bValues) {
        this.PI_X_TAU_A = PI_X_TAU_A;
        this.bValues = bValues;
        getTup2Indices();
    }
    
    private void getTup2Indices() {
        tupId2APGNum = new HashMap<>();
        for (int j = 0; j < PI_X_TAU_A.size64(); j++) {
            ObjectBigArrayBigList oneListInX = PI_X_TAU_A.get(j);
            for (int i = 0; i < oneListInX.size64(); i++) {
                LongBigArrayBigList List1 = (LongBigArrayBigList) oneListInX.get(i);
                for (int k = 0; k < List1.size64(); k++) {
                    long index1 = List1.get(k);
                    tupId2APGNum.put(index1, i);
                }
            }
        }
    }
    
    public int getRepairNum(int limit) {  // returns the number of tuple removals using the greedy approach
        int removedCnt = 0;
        for (int j = 0; j < PI_X_TAU_A.size64(); j++) {
            ObjectBigArrayBigList oneListInX = PI_X_TAU_A.get(j);
            
//            Node tail = getSwapCounts(oneListInX);
            Node tail = getBetterSwapCounts(oneListInX);
            
            tailNode = tail;
//            while (tail != null && tail.prev != null && removedCnt < limit) {
            while (tailNode != null) {
                long tup2go = tailNode.tupId;
                removedCnt++;
                
                if (removedCnt > limit + 10)  // TODO check on this in the future, +10 might use some improvements
                    return -1;
                
                HashSet<Long> updated = getUpdatedTuples(tup2go, oneListInX);
                tailNode.removeThis();
//                tail.removeThis();
//                tail = tail.prev;
//                if (tail == null)
//                    break;
                
//                fixLinkedList(tail, updated);
                fixLinkedList(tailNode, updated);
            }
        }
        
        return removedCnt;
    }
    
    private HashSet<Long> getUpdatedTuples(long tup2go, ObjectBigArrayBigList oneListInX) {
        int aPGIndex = tupId2APGNum.get(tup2go);
        HashSet<Long> toUpdate = new HashSet<>();
    
        for (int i = 0; i < oneListInX.size64(); i++) {
            LongBigArrayBigList List1 = (LongBigArrayBigList) oneListInX.get(i);
            for (long index1 : List1) {
                if ((i < aPGIndex && bValues.get(index1) > bValues.get(tup2go)) || (i > aPGIndex && bValues.get(index1) < bValues.get(tup2go))) {
                    toUpdate.add(index1);
                }
            }
        }
//        System.out.println(tup2go + ": " + toUpdate);
        return toUpdate;
    }
    
    private void fixLinkedList(Node tail, HashSet<Long> toUpdate) {
        ArrayList<Node> nodePointers = new ArrayList<>();
        for (int i = 0; i <= tail.numSwaps; i++) {
            nodePointers.add(null);
        }
        while (tail.prev != null)
            tail = tail.prev;
        
//        Node garbage = tailNode;
//        while (garbage.prev != null)
//            garbage = garbage.prev;
//        System.out.print("before: ");
//        while (garbage != null) {
//            System.out.print(garbage + ", ");
//            garbage = garbage.next;
//        }
//        System.out.println();
        
        Node curNode = tail;
        Node prevNumNode = curNode;  // if curNode.swaps = k, this points to the first node with swap = (k - 1)
        
        while (curNode != null) {
            Node tmpNext = curNode.next;
            if (toUpdate.contains(curNode.tupId)) {
                curNode.numSwaps--;
//                if (tailNode == curNode) {
//                    tailNode = curNode.prev;
//                }
                if (curNode.numSwaps < 1) {
                    curNode.removeThis();
//                    if (prevNumNode == curNode)
//                        prevNumNode = tmpNext;
                } else {
                    if (nodePointers.get(curNode.numSwaps) != null) {
                        curNode.removeThis();
                        nodePointers.get(curNode.numSwaps).addBefore(curNode);
                    } else {
                        nodePointers.set(curNode.numSwaps, curNode);
                        if (nodePointers.get(curNode.numSwaps + 1) != null) {
                            curNode.removeThis();
                            nodePointers.get(curNode.numSwaps + 1).addBefore(curNode);
                        }
                    }
                }
//                else {
//                    if (prevNumNode != curNode) {
//                        prevNumNode.addBefore(curNode);
//                    }
//                }
            } else {
                if (nodePointers.get(curNode.numSwaps) == null) {
                    nodePointers.set(curNode.numSwaps, curNode);
                }
            }
            curNode = tmpNext;
//            if ((curNode != null) && curNode.numSwaps > prevNumNode.numSwaps)
//                prevNumNode = curNode;
        }

//        System.out.print("after: ");
//        if (tailNode != null) {
//            garbage = tailNode;
//            while (garbage.prev != null)
//                garbage = garbage.prev;
//            while (garbage != null) {
//                System.out.print(garbage + ", ");
//                garbage = garbage.next;
//            }
//            System.out.println();
//        }
    }
    
    private Node getSwapCounts(ObjectBigArrayBigList oneListInX) {
        Node curNode = null;
        ArrayList<TupIDSwapPair> swapCounts = new ArrayList<>();  // index i stores the num of swaps of tuple i
        for (int i = 0; i < bValues.size64(); i++) {
            swapCounts.add(i, new TupIDSwapPair(i, 0));
        }
        
        for (int i = 0; i < oneListInX.size64() - 1; i++) {
            LongBigArrayBigList List1 = (LongBigArrayBigList) oneListInX.get(i);
            for (int j = i + 1; j < oneListInX.size64(); j++) {
                LongBigArrayBigList List2 = (LongBigArrayBigList) oneListInX.get(j);
                for (long index1 : List1) {
                    for (long index2 : List2) {
                        if (bValues.get(index1) > bValues.get(index2)) {  // add one swap count to each tuple
                            swapCounts.get((int)index1).swapCnt++;
                            swapCounts.get((int)index2).swapCnt++;
                        }
                    }
                }
            }
        }
        Collections.sort(swapCounts);
        for (TupIDSwapPair swapCount : swapCounts) {
            if (swapCount.swapCnt > 0) {
                if (curNode == null) {
                    curNode = new Node(swapCount.tupId, swapCount.swapCnt, null, null);
                } else {
                    Node n2 = new Node(swapCount.tupId, swapCount.swapCnt, null, curNode);
                    curNode.addAfter(n2);
                    curNode = n2;
                }
            }
        }
        return curNode;
    }
    
    private Node getBetterSwapCounts(ObjectBigArrayBigList oneListInX) {
        Node curNode = null;
        ArrayList<TupIDSwapPair> swapCounts = new ArrayList<>();  // index i stores the num of swaps of tuple i
        for (int i = 0; i < bValues.size64(); i++) {
            swapCounts.add(i, new TupIDSwapPair(i, 0));
        }
        List<TupIDSwapPair> allIdVals = new ArrayList<>();  // TODO: this uses 'swapCount' to store bValue, probably could use some refactoring
        for (int i = 0; i < oneListInX.size64(); i++) {
            LongBigArrayBigList List1 = (LongBigArrayBigList) oneListInX.get(i);
        
            List<TupIDSwapPair> idBs = new ArrayList<>();
            for (long index1 : List1) {
                int value = bValues.get(index1);
                idBs.add(new TupIDSwapPair(index1, value));
            }
            Collections.sort(idBs);
            allIdVals.addAll(idBs);
        }
    
        sortAndCount(allIdVals, swapCounts, 0, allIdVals.size() - 1);
        
        Collections.sort(swapCounts);
        for (TupIDSwapPair swapCount : swapCounts) {
            if (swapCount.swapCnt > 0) {
                if (curNode == null) {
                    curNode = new Node(swapCount.tupId, swapCount.swapCnt, null, null);
                } else {
                    Node n2 = new Node(swapCount.tupId, swapCount.swapCnt, null, curNode);
                    curNode.addAfter(n2);
                    curNode = n2;
                }
            }
        }
        return curNode;
    }
    
    // 'a' and 'b' are both inclusive
    private void sortAndCount(List<TupIDSwapPair> allIdVals, ArrayList<TupIDSwapPair> swapCounts, int a, int b) {
        if (a == b)
            return;
        int m = (a + b) / 2;  // recurse on [a, m] and [m + 1, b]
        sortAndCount(allIdVals, swapCounts, a, m);
        sortAndCount(allIdVals, swapCounts, m + 1, b);
        mergeAndCount(allIdVals, swapCounts, a, m, b);
    }
    
    private void mergeAndCount(List<TupIDSwapPair> allIdVals, ArrayList<TupIDSwapPair> swapCounts, int a, int m, int b) {
        List<TupIDSwapPair> tmpRes = new ArrayList<>();
        int tmpId;
        
        for (int i = a; i <= b; i++) {
            tmpRes.add(null);
        }
        int i = a, j = m + 1, cnt = 0;
        while (i <= m || j <= b) {
            if (i <= m && j > b) {
                tmpRes.set(cnt, allIdVals.get(i));
                tmpId = (int) allIdVals.get(i).tupId;
                swapCounts.get(tmpId).swapCnt += (b - m);
                i++;
                cnt++;
            } else if (i > m && j <= b) {
                tmpRes.set(cnt, allIdVals.get(j));
                j++;
                cnt++;
            } else {
                if (allIdVals.get(i).swapCnt <= allIdVals.get(j).swapCnt) {
                    tmpRes.set(cnt, allIdVals.get(i));
                    tmpId = (int) allIdVals.get(i).tupId;
                    swapCounts.get(tmpId).swapCnt += (j - m - 1);
                    i++;
                } else {
                    tmpRes.set(cnt, allIdVals.get(j));
                    tmpId = (int) allIdVals.get(j).tupId;
                    swapCounts.get(tmpId).swapCnt += (m - i + 1);
                    j++;
                }
                cnt++;
            }
        }
        for (int k = 0; k < tmpRes.size(); k++) {
            allIdVals.set(a + k, tmpRes.get(k));
        }
    }
}

class Node {
    public long tupId;
    public int numSwaps;
    public Node next;
    public Node prev;
    
    public Node(long tupId, int numSwaps, Node next, Node prev) {
        this.tupId = tupId;
        this.numSwaps = numSwaps;
        this.next = next;
        this.prev = prev;
    }
    
    public void addAfter(Node other) {
        if (this.next != null)
            this.next.prev = other;
        other.next = this.next;
        this.next = other;
        other.prev = this;
    }
    
    public void addBefore(Node other) {
        if (this.prev != null)
            this.prev.next = other;
        other.prev = this.prev;
        this.prev = other;
        other.next = this;
    }
    
    public void removeThis() {
        if (next != null)
            next.prev = this.prev;
        if (prev != null)
            prev.next = this.next;
        
        if (CandidateGreedyApprox.tailNode == this)
            CandidateGreedyApprox.tailNode = this.prev;
        
        this.next = null;
        this.prev = null;
    }
    
    @Override
    public String toString() {
        return "{" + tupId +
                ", " + numSwaps +
                '}';
    }
}

class TupIDSwapPair implements Comparable<TupIDSwapPair> {
    public long tupId;
    public int swapCnt;
    
    public TupIDSwapPair(long tupId, int swapCnt) {
        this.tupId = tupId;
        this.swapCnt = swapCnt;
    }
    
    @Override
    public int compareTo(TupIDSwapPair o) {
        return this.swapCnt - o.swapCnt;
    }
    
    @Override
    public String toString() {
        return "{" + tupId +
                ", " + swapCnt +
                '}';
    }
}
