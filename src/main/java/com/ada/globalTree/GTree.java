package com.ada.globalTree;

import com.ada.common.collections.Collections;
import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.common.Constants;
import com.ada.common.Path;
import com.ada.geometry.*;
import org.jetbrains.annotations.NotNull;

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
    transient public int[][] density;

    /**
     * 叶节点ID与叶节点的map映射
     */
    transient public Map<Integer, GDataNode> leafIDMap;

    /**
     * 弃用的叶节点ID，待废弃
     */
    transient public List<Integer> discardLeafIDs;

    private DispatchLeafID dispatchLeafID;

    static class DispatchLeafID{
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
    }

    public boolean check(){
        List<GDataNode> leafs = new ArrayList<>(root.getLeafs());
        if (!Constants.collectionsEqual(leafs, leafIDMap.values()))
            throw new IllegalArgumentException("leafs are not equal.");
//        List<GDataNode> res = Constants.collectDis(leafs);
//        System.out.println("\t" + res.get(0).elemNum + "\t"+ res.get(1).elemNum + "\t"+ res.get(2).elemNum + "\t"+ res.get(3).elemNum + "\t"+ res.get(4).elemNum + "\t");
        return root.check();
    }


    public GTree() {
        root = new GDirNode(null,-1,
                new GridRectangle(new GridPoint(0,0), new GridPoint(Constants.gridDensity,Constants.gridDensity)),
                0,this, new GNode[4]);
        GridRectangle gridRectangle;
        gridRectangle = new GridRectangle(new GridPoint(0,0), new GridPoint(200, 200));
        root.child[0] = new GDataNode(root,0, gridRectangle,0, this,0);
        gridRectangle =  new GridRectangle(new GridPoint(0,201), new GridPoint(200, 511));
        root.child[1] = new GDataNode(root,1, gridRectangle,0, this,1);
        gridRectangle = new GridRectangle(new GridPoint(201,0), new GridPoint(511, 200));
        root.child[2] = new GDataNode(root,2, gridRectangle,0, this,2);
        gridRectangle = new GridRectangle(new GridPoint(201,201), new GridPoint(511, 511));
        root.child[3] = new GDataNode(root,3, gridRectangle,0,  this,3);
        density = new int[Constants.gridDensity+1][Constants.gridDensity+1];
        List<GDataNode> leafs = new ArrayList<>();
        for (GNode leaf : root.child)
            leafs.add((GDataNode) leaf);
        root.setLeafs(leafs);
        dispatchLeafID = new DispatchLeafID();
        discardLeafIDs = new ArrayList<>();
        leafIDMap = new HashMap<>();
        Integer leafID;
        leafID = dispatchLeafID.getLeafID();
        ((GDataNode) root.child[0]).setLeafID(leafID);
        leafIDMap.put(leafID, ((GDataNode) root.child[0]));
        leafID = dispatchLeafID.getLeafID();
        ((GDataNode) root.child[1]).setLeafID(leafID);
        leafIDMap.put(leafID, ((GDataNode) root.child[1]));
        leafID = dispatchLeafID.getLeafID();
        ((GDataNode) root.child[2]).setLeafID(leafID);
        leafIDMap.put(leafID, ((GDataNode) root.child[2]));
        leafID = dispatchLeafID.getLeafID();
        ((GDataNode) root.child[3]).setLeafID(leafID);
        leafIDMap.put(leafID, ((GDataNode) root.child[3]));
    }

    /**
     * 获取一个矩形内的元素数量
     */
    private int getRangeEleNum(GridRectangle range) {
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
        //将GlobalTree中的所有节点的elemNum清零
        root.setAllElemNumZero();

        //更新全局索引的每个节点上的elemNum信息
        updateTreeInfo();

        //获取需要调整结构的子树集合
        Map<GNode, GNode> map = new HashMap<>();
        getAdjustNode(map);

        //更新dirNodes中的每个子树
        adjustNodes(map);
        return map;
    }

    /**
     * 更新全局索引的每个节点上的elemNum信息和网格密度信息
     */
    private void updateTreeInfo() {
        for (GDataNode leaf : leafIDMap.values()) {
            int eleNum = getRangeEleNum(leaf.gridRegion);
            leaf.updateLeafElemNum(eleNum);
        }
    }

    /**
     * 获取需要调整结构的子树集合(GQ)
     * @param nodes 记录需要调整结构的子树
     */
    private void getAdjustNode(Map<GNode, GNode> nodes) {
        leafIDMap.values().forEach(leafNode -> {
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
                List<GNode> descendants = new ArrayList<>();
                boolean hasAncestor = false;
                Path path0 = new Path(addNode);
                for (GNode node : nodes.keySet()) {
                    Path path1 = new Path(node);
                    int tmp = Path.isSameWay(path0, path1);
                    if (tmp == 1)
                        descendants.add(node);
                    if (tmp == -1){
                        hasAncestor = true;
                        break;
                    }
                }
                if (!hasAncestor){
                    for (GNode descendant : descendants)
                        nodes.remove(descendant);
                    nodes.put(addNode, null);
                }
            }
        });
    }

//    /**
//     * 获取需要调整结构的子树集合(QBS)
//     * @param nodes 记录需要调整结构的子树
//     */
//    private void getAdjustNode(Set<GNode> nodes) {
//        leafIDMap.values().forEach(leafNode -> {
//            if ( leafNode.elemNum > 5* Constants.globalLowBound ||
//                    (leafNode.elemNum < Constants.globalLowBound && !leafNode.isRootLeaf()) ) {
//                GNode addNode;
//                if (leafNode.elemNum < 5 * Constants.globalLowBound){ //叶节点不可以分裂
//                    addNode = leafNode.parent;
//                    while (true) {
//                        if (addNode.isRoot()){
//                            break;
//                        }else if (addNode.elemNum < Constants.globalLowBound) {
//                            addNode = addNode.parent;
//                        }else if (addNode.elemNum <= 2* Constants.globalLowBound ){
//                            break;
//                        }else if ( addNode.elemNum < 5 * Constants.globalLowBound ) {
//                            addNode = addNode.parent;
//                        }else {
//                            break;
//                        }
//                    }
//                }else { //叶节点可以分裂
//                    addNode = leafNode;
//                }
//                List<GNode> descendants = new ArrayList<>();
//                boolean hasAncestor = false;
//                Path path0 = new Path(addNode);
//                for (GNode node : nodes) {
//                    Path path1 = new Path(node);
//                    int tmp = Path.isSameWay(path0, path1);
//                    if (tmp == 1){
//                        descendants.add(node);
//                    }
//                    if (tmp == -1){
//                        hasAncestor = true;
//                        break;
//                    }
//                }
//                if (!hasAncestor){
//                    nodes.removeAll(descendants);
//                    nodes.add(addNode);
//                }
//            }
//        });
//    }

    /**
     * 更新dirNodes中的每个子树
     */
    private void adjustNodes(@NotNull Map<GNode, GNode> nodes) {
        for (GNode node : nodes.keySet()) {
            GDataNode dataNode = new GDataNode(node.parent, node.position, node.gridRegion,
                    node.elemNum, node.tree,-1);
            GNode newNode = dataNode.adjustNode();
            newNode.countLeafs();
            if (node.isRoot()) {
                root = (GDirNode) newNode;
            }else {
                node.parent.child[node.position] = newNode;
                node.parent.alterLeafs(node.getLeafs(), newNode.getLeafs());
            }
            dispatchLeafID(node, newNode);
            nodes.replace(node, newNode);
        }
    }

    /**
     * 子树调整前后的节点是oldNode，newNode。为newNode中的叶节点重新分配ID
     */
    private void dispatchLeafID(GNode oldNode, GNode newNode){
        if (oldNode instanceof GDirNode && newNode instanceof GDirNode){ //多分多
            GDirNode newDirNode = (GDirNode) newNode;
            GDirNode oldDirNode = (GDirNode) oldNode;
            List<GDataNode> newLeafNodes = new ArrayList<>(newDirNode.getLeafs());
            List<GDataNode> oldLeafNodes = new ArrayList<>(oldDirNode.getLeafs());
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
            int[][] leafIDMap = Constants.redisPatchLeafID(matrix, 100*globalLowBound);
            for (int[] map : leafIDMap) {
                int leafId = oldLeafNodes.get(map[1]).leafID;
                newLeafNodes.get(map[0]).setLeafID(leafId);
                this.leafIDMap.put(leafId, newLeafNodes.get(map[0]));
            }
            if (newLeafNodes.size() > oldLeafNodes.size()){ //少分多
                Set<Integer> reassigningID = new HashSet<>();
                for (int[] map : leafIDMap)
                    reassigningID.add(map[0]);
                for (int i = 0; i < newLeafNodes.size(); i++) {
                    if (!reassigningID.contains(i)){
                        Integer leafID = dispatchLeafID.getLeafID();
                        newLeafNodes.get(i).setLeafID(leafID);
                        discardLeafIDs.remove(leafID);
                        this.leafIDMap.put(leafID, newLeafNodes.get(i));
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
                        discardLeafIDs.add(leafID);
                        dispatchLeafID.discardLeafID(leafID);
                    }
                }
            }
        }else if(oldNode instanceof GDataNode && newNode instanceof GDirNode){ //一分多
            GDirNode newDirNode = (GDirNode) newNode;
            List<GDataNode> newLeafNodes = new ArrayList<>(newDirNode.getLeafs());
            int maxNumLeaf = getMaxElemNumIndex(newLeafNodes);
            Integer leafID = ((GDataNode)oldNode).leafID;
            newLeafNodes.get(maxNumLeaf).setLeafID(leafID);
            this.leafIDMap.put(leafID, newLeafNodes.get(maxNumLeaf));
            newLeafNodes.remove(maxNumLeaf);
            for (GDataNode newLeafNode : newLeafNodes) {
                leafID = dispatchLeafID.getLeafID();
                newLeafNode.setLeafID(leafID);
                discardLeafIDs.remove(leafID);
                this.leafIDMap.put(newLeafNode.leafID, newLeafNode);
            }
        }else if(oldNode instanceof GDirNode && newNode instanceof GDataNode){ //多合一
            GDirNode oldDirNode = (GDirNode) oldNode;
            List<GDataNode> oldLeafNodes = new ArrayList<>(oldDirNode.getLeafs());
            int maxNumLeaf = getMaxElemNumIndex(oldLeafNodes);
            Integer leafID = oldLeafNodes.get(maxNumLeaf).leafID;
            ((GDataNode)newNode).setLeafID(leafID);
            this.leafIDMap.put(leafID, (GDataNode)newNode);
            oldLeafNodes.remove(maxNumLeaf);
            for (GDataNode oldLeafNode : oldLeafNodes) {
                leafID = oldLeafNode.leafID;
                discardLeafIDs.add(leafID);
                dispatchLeafID.discardLeafID(leafID);
            }
        }else {
            throw new IllegalArgumentException("GNode type error.");
        }
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

}










