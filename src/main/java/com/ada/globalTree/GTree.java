package com.ada.globalTree;

import com.ada.Hungarian.Hungary;
import com.ada.common.Constants;
import com.ada.common.Path;
import com.ada.common.collections.Collections;
import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.geometry.Rectangle;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.*;

public class GTree implements Serializable {

    /**
     * 根节点
     */
    private GDirNode root;

    /**
     * 全局索引叶节点索引项数量的下届
     */
    public int globalLowBound;

    /**
     * 密度网格
     */
    public transient int[][] density;

    public int leafNum;

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
        leafNum = 4;
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

    static class DispatchLeafID implements Serializable{
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

    public boolean check(){
        List<GDataNode> leafs = new ArrayList<>();
        root.getLeafs(leafs);
        return root.check();
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
        int itemNum = getRangeEleNum(new GridRectangle(new GridPoint(0, 0), new GridPoint(Constants.gridDensity, Constants.gridDensity)));
        double factor = 0.9;
        globalLowBound = (int)((itemNum/Constants.dividePartition)*0.8);
        Map<GNode, GNode> map = new HashMap<>();
        int leafNum;

        do {
            factor += 0.1;
            globalLowBound = (int) (factor * globalLowBound);
            map.clear();
            leafNum = this.leafNum;
            List<GNode> list = new ArrayList<>();
            getAdjustNode(list);
            for (GNode node : list) {
                GDataNode dataNode = new GDataNode(node.parent, node.position, node.gridRegion,
                        node.elemNum, node.tree, -1);
                GNode newNode = dataNode.adjustNode();
                List<GDataNode> oldLeaves = new ArrayList<>();
                List<GDataNode> newLeaves = new ArrayList<>();
                newNode.getLeafs(newLeaves);
                node.getLeafs(oldLeaves);
                leafNum += newLeaves.size() - oldLeaves.size();
                map.put(node, newNode);
            }
        }while (leafNum > Constants.dividePartition);
        this.leafNum = leafNum;
        map.forEach((oldNode, newNode) -> {
            if (oldNode.isRoot()) {
                root = (GDirNode) newNode;
            }else {
                oldNode.parent.child[oldNode.position] = newNode;
            }
            dispatchLeafID(oldNode, newNode);
        });
        return map;
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
}










