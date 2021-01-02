package com.ada.geometry;

import com.ada.common.SortList;

import java.io.Serializable;
import java.util.*;

/**
 * task中所有内部轨迹的相似性度量的中间结果缓存类
 */
public class TwoDimSimilarState implements Serializable {
    public Map<Integer, SortList<SimilarState>> comparingInfo = new HashMap<>();
    public Map<Integer,List<Integer>> comparedInfo = new HashMap<>();

    /**
     * 移除轨迹key的相似度中间状态，包括TID作为比较轨迹和被比较轨迹
     */
    public void removeState(int key){
        SortList<SimilarState> sortList = comparingInfo.get(key);
        if (sortList != null) {
            for (SimilarState state : sortList.getList())
                comparedInfo.get(state.comparedTID).remove(Integer.valueOf(key));
            comparingInfo.remove(key);
        }
        if (comparedInfo.containsKey(key)) {
            List<Integer> list = comparedInfo.get(key);
            for (Integer integer : list) {
                SortList<SimilarState> sortList1 = comparingInfo.get(integer);
                if (sortList1 == null)
                    throw new IllegalArgumentException("Error");
                else
                    sortList1.remove(new SimilarState(integer, key, null, null));
            }
            comparedInfo.remove(key);
        }
    }

    /**
     * 获取轨迹TID作为比较轨迹的相似度中间状态
     */
    public List<SimilarState> getTIDState(Integer TID) {
        SortList<SimilarState> sortList = comparingInfo.get(TID);
        if (sortList == null)
            return null;
        else
            return sortList.getList();
    }

    /**
     * 获取轨迹key的相似度中间状态，包括key是比较轨迹和被比较轨迹的两种情况。
     */
    public List<SimilarState> getTIDAllState(int key){
        SortList<SimilarState> sortList = comparingInfo.get(key);
        List<SimilarState> list;
        if (sortList == null)
            list = new ArrayList<>();
        else
            list =  new ArrayList<>(sortList.getList());
        if ( comparedInfo.get(key) != null) {
            for (Integer integer : comparedInfo.get(key)) {
                SortList<SimilarState> sortList1 = comparingInfo.get(integer);
                int site = sortList1.search(new SimilarState(0, key, null, null));
                list.add(sortList1.get(site));
            }
        }
        return list;
    }

    /**
     * 计算完轨迹的相似度后，将需要记录轨迹key的中间结果插入到中间结果缓存类中。
     * 如果中间结果缓存类中已经有key的中间结果时，做更新操作，即删除无用的添加没有的
     * 中间结果。
     * @param tid 轨迹ID
     * @param newList 新的中间结果
     */
    public void insertTIDState(int tid, SortList<SimilarState> newList){
        SortList<SimilarState> sortList = comparingInfo.get(tid);
        if (sortList == null) { //之前没有存储轨迹tid的相似度计算中间状态
            comparingInfo.put(tid, newList);
            comparedInfo.computeIfAbsent(tid, k -> new ArrayList<>());
            for (SimilarState state : newList.getList()) {
                Integer comparedTID = state.comparedTID;
                List<Integer> list = comparedInfo.get(comparedTID);
                if (list != null)
                    list.add(state.comparingTID);
                comparedInfo.computeIfAbsent(comparedTID, k -> new ArrayList<>(Collections.singletonList(state.comparingTID)));
            }
        }else{
            Set<SimilarState> minus = new HashSet<>(sortList.getList());
            Set<SimilarState> add = new HashSet<>(newList.getList());
            minus.removeAll(add);
            add.removeAll( new HashSet<>(sortList.getList()));
            sortList.removeAll(minus);
            sortList.addAll(add);
            for (SimilarState state: minus)
                comparedInfo.get(state.comparedTID).remove(Integer.valueOf(tid));
            for (SimilarState state: add) {
                List<Integer> list = comparedInfo.get(state.comparedTID);
                if (list != null)
                    list.add(tid);
                comparedInfo.computeIfAbsent(state.comparedTID, integer -> new ArrayList<>(Collections.singletonList(tid)));
            }
        }
    }

    /**
     * 查看缓存中是否有轨迹TID作为比较轨迹的相似度中间状态
     */
    public boolean contains(Integer TID) {
        return comparingInfo.containsKey(TID);
    }


}
