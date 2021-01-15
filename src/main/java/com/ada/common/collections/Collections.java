package com.ada.common.collections;


import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Collections {

    @SuppressWarnings("unchecked")
    public static <FROM, TO>
        Collection<TO> changeCollectionElem(Collection<FROM> collection,
                                            ChangeAction<FROM, TO> changeAction){
        Collection<TO> out = null;
        try {
            out = collection.getClass().newInstance();
            for (FROM from : collection)
                out.add(changeAction.action(from));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    public static <T> Collection<T> removeAndGatherElms(Collection<T> collection, Judge<T> judge) throws IllegalAccessException, InstantiationException {
        Collection<T> out = collection.getClass().newInstance();
        collection.removeIf(t -> {
            if (judge.action(t)){
                out.add(t);
                return true;
            }else {
                return false;
            }
        });
        return out;
    }

    public static <T> boolean collectionsEqual(Collection<T> col1, Collection<T> col2) {
        if (col1 == null && col2 == null)
            return true;
        if (col1 == null)
            return false;
        if (col2 == null)
            return false;
        Set<T> set1 = new HashSet<>(col1);
        Set<T> set2 = new HashSet<>(col2);
        set1.removeAll(col2);
        set2.removeAll(col1);
        if (!set1.isEmpty())
            return false;
        return set2.isEmpty();
    }

    public static <K, V> boolean mapEqual(Map<K, V> map1, Map<K, V> map2){
        if (map1 == null && map2 == null)
            return true;
        if (map1 == null)
            return false;
        if (map2 == null)
            return false;
        if (map1.size() != map2.size())
            return false;
        boolean flag = true;
        for (Map.Entry<K, V> entry : map1.entrySet()) {
            V v = map2.get(entry.getKey());
            if (v == null){
                flag = false;
                break;
            }
            if (!v.equals(entry.getValue())){
                flag = false;
                break;
            }
        }
        return flag;
    }
}






















