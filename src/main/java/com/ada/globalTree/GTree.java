package com.ada.globalTree;

import com.ada.DTflinkFunction.DTConstants;
import com.ada.Hungarian.Hungary;
import com.ada.common.collections.Collections;
import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.common.Constants;
import com.ada.common.Path;
import com.ada.geometry.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.*;

public class GTree {

    /**
     * 根节点
     */
    public GDirNode root;

    /**
     * 全局索引叶节点索引项数量的下届
     */
    public static int globalLowBound;

    /**
     * 密度网格
     */
    public transient int[][] density;

    /**
     * 分配叶节点ID的功能成员
     */
    DispatchLeafID dispatchLeafID;

    public GTree() {
        dispatchLeafID = new DispatchLeafID();
        int gridDensity = Constants.gridDensity;
        root = new GDirNode(null,-1,
                new GridRectangle(new GridPoint(0,0), new GridPoint(gridDensity, gridDensity)),
                0,this, new GNode[4]);
        GridRectangle gridRectangle;
        gridRectangle = new GridRectangle(new GridPoint(0,0), new GridPoint(gridDensity/2, gridDensity/2));
        root.child[0] = new GDataNode(root,0, gridRectangle,0, this,dispatchLeafID.getLeafID());
        gridRectangle =  new GridRectangle(new GridPoint(0,(gridDensity/2)+1), new GridPoint(gridDensity/2, gridDensity));
        root.child[1] = new GDataNode(root,1, gridRectangle,0, this,dispatchLeafID.getLeafID());
        gridRectangle = new GridRectangle(new GridPoint((gridDensity/2)+1,0), new GridPoint(gridDensity, (gridDensity/2)));
        root.child[2] = new GDataNode(root,2, gridRectangle,0, this,dispatchLeafID.getLeafID());
        gridRectangle = new GridRectangle(new GridPoint((gridDensity/2)+1,(gridDensity/2)+1), new GridPoint(gridDensity, gridDensity));
        root.child[3] = new GDataNode(root,3, gridRectangle,0,  this,dispatchLeafID.getLeafID());
        density = new int[gridDensity+1][gridDensity+1];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GTree gTree = (GTree) o;
        if (!Objects.equals(root, gTree.root))
            return false;
//        if (!com.ada.common.Arrays.arrsEqual(density, gTree.density))
//            return false;
        if (!Objects.equals(dispatchLeafID, gTree.dispatchLeafID))
            return false;
        return true;
    }

    static class DispatchLeafID implements Serializable {
        List<Integer> usedLeafID;

        List<Integer> canUseLeafID;

        DispatchLeafID(){
            usedLeafID = new ArrayList<>();
            canUseLeafID = new ArrayList<>();
            for (int i = Constants.dividePartition-1; i >= 0; i--)
                canUseLeafID.add(i);
        }

        Integer getLeafID(){
            if (canUseLeafID.isEmpty()){
                throw new IllegalArgumentException("LeafID is FPed");
            }else {
                Integer leafID = canUseLeafID.remove(canUseLeafID.size() - 1);
                usedLeafID.add(leafID);
                return leafID;
            }
        }

        void discardLeafID(Integer leafID){
            canUseLeafID.add(leafID);
            usedLeafID.remove(leafID);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DispatchLeafID that = (DispatchLeafID) o;
            if (!Collections.collectionsEqual(usedLeafID, that.usedLeafID))
                return false;
            if (!Collections.collectionsEqual(canUseLeafID, that.canUseLeafID))
                return false;
            return true;
        }
    }

    public boolean check(Map<Integer, TrackKeyTID> trackMap){
        return root.check(trackMap);
    }


    /**
     * 获取一个矩形内的元素数量
     */
    int getRangeEleNum(GridRectangle range) {
        int res = 0;
        for (int i = range.low.x; i <= range.high.x; i++) {
            for (int j = range.low.y; j <= range.high.y; j++) {
                res += density[i][j];
            }
        }
        return res;
    }

    /**
     * 获取矩形范围region内某个坐标方向上的索引项密度
     * @param region 矩形范围
     * @param axis 0 获取x轴方向的数据密度， 1 获取y轴方向上的数据密度
     * @return 数据密度
     */
    int[] getElemNumArray(GridRectangle region, int axis) {
        int[] res;
        int tmp;
        if (axis == 0){
            res = new int[region.high.x - region.low.x + 1];
            for (int i = region.low.x; i <= region.high.x; i++) {
                tmp = 0;
                for (int j = region.low.y; j <= region.high.y; j++)
                    tmp += density[i][j];
                res[i - region.low.x] = tmp;
            }
        }else {
            res = new int[region.high.y - region.low.y + 1];
            for (int i = region.low.y; i <= region.high.y; i++) {
                tmp = 0;
                for (int j = region.low.x; j <= region.high.x; j++)
                    tmp += density[j][i];
                res[i - region.low.y] = tmp;
            }
        }
        return res;
    }

    /**
     * 根据新的网格密度数据更新树结构
     */
    public Map<GNode, GNode> updateTree(){
        //更新全局索引的每个节点上的elemNum信息
        root.updateElemNum();

        //获取需要调整结构的子树集合
        List<GNode> list = new ArrayList<>();
        getAdjustNode(list);

        //更新dirNodes中的每个子树
        return adjustNodes(list);
    }

    /**
     * 获取需要调整结构的子树集合(GQ)
     * @param nodes 记录需要调整结构的子树
     */
    private void getAdjustNode(List<GNode> nodes) {
        for (GDataNode leafNode : getAllLeafs()) {
            if ( leafNode.elemNum > 2*globalLowBound ||
                    (leafNode.elemNum < globalLowBound && !leafNode.isRootLeaf()) ) {
                GNode addNode;
                if (leafNode.elemNum < 4.5 * globalLowBound){ //叶节点不可以分裂
                    addNode = leafNode.parent;
                    while (true) {
                        if (addNode.isRoot()){
                            break;
                        }else if (addNode.elemNum < globalLowBound) {
                            addNode = addNode.parent;
                        }else if (addNode.elemNum <= 2* globalLowBound ){
                            break;
                        }else if ( addNode.elemNum < 4.5 * globalLowBound ) {
                            addNode = addNode.parent;
                        }else {
                            break;
                        }
                    }
                }else { //叶节点可以分裂
                    addNode = leafNode;
                }
                boolean hasAncestor = false;
                Path path0 = new Path(addNode);
                for (Iterator<GNode> ite = nodes.iterator(); ite.hasNext(); ){
                    GNode node = ite.next();
                    Path path1 = new Path(node);
                    int tmp = Path.isSameWay(path0, path1);
                    if (tmp == 1) ite.remove();
                    if (tmp == -1){
                        hasAncestor = true;
                        break;
                    }
                }
                if (!hasAncestor) nodes.add(addNode);
            }
        }
    }

    /**
     * 更新dirNodes中的每个子树
     */
    private Map<GNode, GNode> adjustNodes(List<GNode> nodes) {
        Map<GNode, GNode> map = new HashMap<>();
        for (GNode node : nodes) {
            GDataNode dataNode = new GDataNode(node.parent, node.position, node.gridRegion,
                    node.elemNum, node.tree,-1);
            GNode newNode = dataNode.adjustNode();
            if (node.isRoot()) {
                root = (GDirNode) newNode;
            }else {
                node.parent.child[node.position] = newNode;
            }
            dispatchLeafID(node, newNode);
            map.put(node, newNode);
        }
        return map;
    }

    /**
     * 子树调整前后的节点是oldNode，newNode。为newNode中的叶节点重新分配ID
     */
    private void dispatchLeafID(GNode oldNode, GNode newNode){
        if (oldNode instanceof GDirNode && newNode instanceof GDirNode){ //多分多
            GDirNode newDirNode = (GDirNode) newNode;
            GDirNode oldDirNode = (GDirNode) oldNode;
            List<GDataNode> newLeafNodes = new ArrayList<>();
            newDirNode.getLeafs(newLeafNodes);
            List<GDataNode> oldLeafNodes = new ArrayList<>();
            oldDirNode.getLeafs(oldLeafNodes);
            int[][] matrix = new int[newLeafNodes.size()][oldLeafNodes.size()];
            for (int i = 0; i < matrix.length; i++) {
                GDataNode dataNode = newLeafNodes.get(i);
                GridPoint gPoint = new GridPoint();
                for (int j = dataNode.gridRegion.low.x; j <= dataNode.gridRegion.high.x; j++) {
                    for (int k = dataNode.gridRegion.low.y; k <= dataNode.gridRegion.high.y; k++) {
                        gPoint.x = j;
                        gPoint.y = k;
                        GDataNode gDataNode = oldDirNode.searchGPoint(gPoint);
                        int index = oldLeafNodes.indexOf(gDataNode);
                        matrix[i][index] += density[j][k];
                    }
                }
            }
            int[][] leafIDMap = redisPatchLeafID(matrix, 100*globalLowBound);
            for (int[] map : leafIDMap) {
                int leafId = oldLeafNodes.get(map[1]).leafID;
                newLeafNodes.get(map[0]).setLeafID(leafId);
            }
            if (newLeafNodes.size() > oldLeafNodes.size()){ //少分多
                Set<Integer> reassigningID = new HashSet<>();
                for (int[] map : leafIDMap)
                    reassigningID.add(map[0]);
                for (int i = 0; i < newLeafNodes.size(); i++) {
                    if (!reassigningID.contains(i)){
                        Integer leafID = dispatchLeafID.getLeafID();
                        newLeafNodes.get(i).setLeafID(leafID);
                    }
                }
            }
            if (newLeafNodes.size() < oldLeafNodes.size()){ //多分少
                Set<Integer> reassignedID = new HashSet<>();
                for (int[] map : leafIDMap)
                    reassignedID.add(map[1]);
                for (int i = 0; i < oldLeafNodes.size(); i++) {
                    if (!reassignedID.contains(i)){
                        Integer leafID = oldLeafNodes.get(i).leafID;
                        dispatchLeafID.discardLeafID(leafID);
                    }
                }
            }
        }else if(oldNode instanceof GDataNode && newNode instanceof GDirNode){ //一分多
            GDirNode newDirNode = (GDirNode) newNode;
            List<GDataNode> newLeafNodes = new ArrayList<>();
            newDirNode.getLeafs(newLeafNodes);
            int maxNumLeaf = getMaxElemNumIndex(newLeafNodes);
            Integer leafID = ((GDataNode)oldNode).leafID;
            newLeafNodes.get(maxNumLeaf).setLeafID(leafID);
            newLeafNodes.remove(maxNumLeaf);
            for (GDataNode newLeafNode : newLeafNodes) {
                leafID = dispatchLeafID.getLeafID();
                newLeafNode.setLeafID(leafID);
            }
        }else if(oldNode instanceof GDirNode && newNode instanceof GDataNode){ //多合一
            GDirNode oldDirNode = (GDirNode) oldNode;
            List<GDataNode> oldLeafNodes = new ArrayList<>();
            oldDirNode.getLeafs(oldLeafNodes);
            int maxNumLeaf = getMaxElemNumIndex(oldLeafNodes);
            int leafID = oldLeafNodes.get(maxNumLeaf).leafID;
            ((GDataNode)newNode).setLeafID(leafID);
            oldLeafNodes.remove(maxNumLeaf);
            for (GDataNode oldLeafNode : oldLeafNodes) dispatchLeafID.discardLeafID(oldLeafNode.leafID);
        }else {
            throw new IllegalArgumentException("GNode type error.");
        }
    }


    /**
     * 使用Hungarian Algorithm重新分配leafID.
     * @param matrix 分裂前后元素映射数量关系
     * @return 分配结果
     */
    public static int[][] redisPatchLeafID(int[][] matrix, int upBound){
        int rowNum = matrix.length;
        int colNum = matrix[0].length;
        int[][] newMatrix;
        if (rowNum > colNum){
            newMatrix = new int[rowNum][rowNum];
            for (int i = 0; i < newMatrix.length; i++) {
                for (int j = 0; j < newMatrix[0].length; j++) {
                    if (j >= matrix[0].length)
                        newMatrix[i][j] = upBound;
                    else
                        newMatrix[i][j] = upBound - matrix[i][j];
                }
            }
        }else {
            newMatrix = new int[colNum][];
            for (int i = 0; i < newMatrix.length; i++) {
                if (i < matrix.length) {
                    newMatrix[i] = new int[colNum];
                    for (int j = 0; j < colNum; j++)
                        newMatrix[i][j] = upBound - matrix[i][j];
                }else {
                    newMatrix[i] = new int[colNum];
                    Arrays.fill(newMatrix[i],upBound);
                }
            }
        }
        int[][] res = new Hungary().calculate(newMatrix);
        List<Tuple2<Integer,Integer>> list = new ArrayList<>();
        for (int[] re : res)
            list.add(new Tuple2<>(re[0],re[1]));
        if (rowNum > colNum)
            list.removeIf(ints -> ints.f1 >= colNum);
        else
            list.removeIf(ints -> ints.f0 >= rowNum);
        res = new int[list.size()][2];
        for (int i = 0; i < res.length; i++) {
            res[i][0] = list.get(i).f0;
            res[i][1] = list.get(i).f1;
        }
        return res;
    }


    /**
     * 在叶节点集合leafNodes中找出elemNum最大的节点
     */
    private int getMaxElemNumIndex(List<GDataNode> leafNodes) {
        int maxNum = -1;
        int maxIndex = -1;
        for (int i = 0; i < leafNodes.size(); i++) {
            int currentNum = leafNodes.get(i).elemNum;
            if (currentNum > maxNum){
                maxIndex = i;
                maxNum = currentNum;
            }
        }
        return maxIndex;
    }

    /**
     * 返回树中与rectangle相交的叶节点的ID集合
     */
    public List<Integer> searchLeafNodes(Rectangle rectangle){
        List<GDataNode> list = getIntersectLeafNodes(rectangle);
        return (List<Integer>) Collections.changeCollectionElem(list, node -> node.leafID);
    }

    /**
     * 返回树中与rectangle相交的叶节点集合
     */
    public List<GDataNode> getIntersectLeafNodes(Rectangle rectangle){
        List<GDataNode> list = new ArrayList<>();
        root.getIntersectLeafNodes(rectangle, list);
        return list;
    }

    public List<GDataNode> getAllLeafs(){
        List<GDataNode> list = new ArrayList<>();
        root.getLeafs(list);
        return list;
    }

    /**
     * 计算track的经过分区passP、topK分区track.topKP、和enlargeTuple
     */
    public void countPartitions(Rectangle MBR, TrackKeyTID track) {
        root.getIntersectLeafNodes(MBR, track.passP);
        List<GDataNode> topKLeafs = new ArrayList<>();
        root.getIntersectLeafNodes(track.rect, topKLeafs);
        countEnlargeBound(track, new ArrayList<>(topKLeafs), MBR);
        topKLeafs.removeAll(track.passP);
        if (!topKLeafs.isEmpty()) {
            List<GLeafAndBound> list = new ArrayList<>(topKLeafs.size());
            for (GDataNode leaf : topKLeafs) {
                double bound = Constants.countEnlargeBound(MBR, leaf.region);
                list.add(new GLeafAndBound(leaf, bound));
            }
            track.topKP.setList(list);
        }
    }

    public void countTopKAndEnlargeBound(TrackKeyTID track, List<GDataNode> MBRLeafs, List<GDataNode> pruneAreaLeafs, Rectangle MBR) {
        countEnlargeBound(track, pruneAreaLeafs, MBR);
        pruneAreaLeafs.removeAll(MBRLeafs);
        List<GLeafAndBound> list = new ArrayList<>(pruneAreaLeafs.size());
        for (GDataNode leaf : pruneAreaLeafs) {
            double bound = Constants.countEnlargeBound(MBR, leaf.region);
            list.add(new GLeafAndBound(leaf,bound));
        }
        track.topKP.setList(list);
    }

    /**
     * 计算轨迹track的阈值的最大扩展数
     * @param removeLeafs 移除的节点
     */
    public void countEnlargeBound(TrackKeyTID track, List<GDataNode> removeLeafs, Rectangle MBR) {
        Rectangle pruneArea;
        if (root.region.isInternal(track.rect))
            pruneArea = track.rect;
        else
            pruneArea = track.rect.createIntersection(root.region);
        GNode node = root.getInternalNode(pruneArea);
        List<GDataNode> nodeLeafs = new ArrayList<>();
        node.getLeafs(nodeLeafs);
        nodeLeafs.removeAll(removeLeafs);
        track.enlargeTuple.f0 = node;
        Rectangle newRect = node.region.extendToEnoughBig();
        track.enlargeTuple.f1 = Constants.countEnlargeOutBound(MBR, newRect);
        for (GDataNode leaf : nodeLeafs) {
            double enlargeBound = Constants.countEnlargeBound(MBR, leaf.region);
            if (enlargeBound < track.enlargeTuple.f1) {
                track.enlargeTuple.f0 = leaf;
                track.enlargeTuple.f1 = enlargeBound;
            }
        }
    }


    /**
     * 轨迹track的topKP变多，需要重新计算topKP和enlargeTuple
     * @return 扩展的topKP
     */
    public List<GDataNode> enlargePartitions(TrackKeyTID track, Rectangle MBR) {
        List<GDataNode> leafs = new ArrayList<>();
        root.getIntersectLeafNodes(track.rect, leafs);
        countEnlargeBound(track, new ArrayList<>(leafs), MBR);
        leafs.removeAll(track.passP);
        for (GLeafAndBound lb : track.topKP.getList())
            leafs.remove(lb.leaf);
        if (!leafs.isEmpty())
            DTConstants.addTrackTopK(track,MBR, leafs);
        return leafs;
    }
}










