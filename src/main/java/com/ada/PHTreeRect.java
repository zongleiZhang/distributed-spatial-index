package com.ada;

import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import org.tinspin.index.phtree.PHTreeR;

import java.util.ArrayList;
import java.util.List;

public class PHTreeRect {
    PHTreeR<List<Segment>> tree;

    public PHTreeRect(){
        tree = PHTreeR.createPHTree(2);
    }

    public void insert(Segment segment) {
        List<Segment> value = tree.queryExact(segment.rect.low.data, segment.rect.high.data);
        if (value == null){
            value = new ArrayList<>(4);
            value.add(segment);
            tree.insert(segment.rect.low.data, segment.rect.high.data, value);
        }else {
            value.add(segment);
        }
    }


    public void delete(Segment segment) {
        List<Segment> value = tree.queryExact(segment.rect.low.data, segment.rect.high.data);
        if (value != null){
            value.remove(segment);
            if (value.size() == 0)
                tree.remove(segment.rect.low.data, segment.rect.high.data);
        }
    }


    public List<Segment> search(Rectangle rect) {
        List<Segment> list = new ArrayList<>();
        tree.queryIntersect(rect.low.data, rect.high.data).forEachRemaining(entry -> list.addAll(entry.value()));
        return list;
    }
}
