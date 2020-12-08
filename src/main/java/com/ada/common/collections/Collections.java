package com.ada.common.collections;


import java.util.Collection;

public class Collections {

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
}
