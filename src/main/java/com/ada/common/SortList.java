package com.ada.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SortList<T extends Comparable<T>> implements Serializable {
    private List<T> list;

    public SortList() {}

    public SortList(List<T> list) {
        Collections.sort(list);
        this.list = list;
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        Collections.sort(list);
        this.list = list;
    }

    public boolean isEmpty(){
        return list == null || list.isEmpty();
    }

    public void add(T t){
        int site = search(t,true);
        list.add(site,t);
    }

    public void addAll(Collection<T> collection){
        for (T t:collection)
            add(t);
    }

    public T remove(T t){
        int site = search(t,false);
        if (site != -1)
            return list.remove(site);
        else
            return null;
    }

    public T remove(int t){
         if (t < 0 || t > list.size())
             return null;
         return list.remove(t);
    }

    public List<T> removeBigger(T t){
        List<T> res = new ArrayList<>();
        for (int i = list.size()-1; i > -1; i--) {
            T tt = list.get(i);
            if (tt.compareTo(t) > 0) {
                res.add(tt);
                list.remove(i);
            }else {
                return res;
            }
        }
        list = null;
        return res;
    }

    public void removeAll(Collection<T> collection){
        for (T t:collection)
            remove(t);
    }

    public T get(int i){
        return list.get(i);
    }

    public T get(T t){
        return list.get(list.indexOf(t));
    }

    public T getLast(){
        return list.get(list.size()-1);
    }

    public T getFirst(){
        return  list.get(0);
    }

    public int search(T key){
        return search(key,false);
    }

    /**
     * 查找key在排序表中所属的位置。如果是为了插入而查找，返回key应该插入的位置。如果不是为了插入而查找，
     * 当列表中不存在key时返回-1，列表中存在key时，返回key所在的位置。
     * @param isInsert true为了插入而查找，false不是为了插入而查找。
     */
    private int search(T key, boolean isInsert) {
        int start = 0;
        int end = list.size() - 1;
        int middle = (start + end) / 2;
        while (start <= end) {
            if ( key.compareTo(list.get(middle)) < 0) {
                end = middle - 1;
            }else if ( key.compareTo(list.get(middle)) > 0 ) {
                start = middle + 1;
            }else {
                return middle;
            }
            middle = (start + end) / 2;
        }
        if (isInsert)
            return start;
        else
            return -1;
    }


    public void clear() {
        list = null;
    }
}
