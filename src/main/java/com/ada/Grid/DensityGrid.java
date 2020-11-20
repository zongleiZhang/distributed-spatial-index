package com.ada.Grid;

public class DensityGrid{
//    public int[][] data;
//    public GridDirNode root;
//
//
//    public DensityGrid(){}
//
//    public DensityGrid(GridRectangle gr){
//        Rectangle rectangle = gr.toRectangle();
//        this.data = new int[gr.high.x - gr.low.x + 1][];
//        for (int i = 0; i < gr.high.x - gr.low.x + 1; i++) {
//            data[i] = new int[gr.high.y - gr.low.y + 1];
//        }
//        this.root = new GridDirNode( gr,-1, -1, null );
////        this.root = (GridDirNode) generateGridTree(rect, gr);
//    }
//
//    public int[][] getData() {
//        return data;
//    }
//
//    public void setData(int[][] data) {
//        this.data = data;
//    }
//
//    public GridDirNode getRoot() {
//        return root;
//    }
//
//    public void setRoot(GridDirNode root) {
//        this.root = root;
//    }
//
//
//    /**
//     * 创建树
//     */
//    private GridNode generateGridTree(Rectangle rectangle, GridRectangle gr){
//        if (gr.low.x == gr.high.x && gr.low.y == gr.high.y){
//             return new GridDataNode(/*rect, */gr);
//        }else if(gr.low.x == gr.high.x){
//            int middleY = (gr.low.y + gr.high.y)/2;
//            double latBoundary = (rectangle.high.data[1] + rectangle.low.data[1])/2;
//            Rectangle rectangle0 = new Rectangle(rectangle.low,
//                    new Point(new double[]{rectangle.high.data[0], latBoundary}));
//            Rectangle rectangle1 = new Rectangle(new Point(new double[]{rectangle.low.data[0], latBoundary}),
//                    rectangle.high);
//            GridNode[] child = new GridNode[4];
//            child[0] = generateGridTree(rectangle0, new GridRectangle(gr.low, new GridPoint(gr.high.x, middleY)));
//            child[1] = generateGridTree(rectangle1, new GridRectangle(new GridPoint(gr.low.x, middleY+1), gr.high));
//            child[2] = null;
//            child[3] = null;
//            return  new GridDirNode(/*rect, */gr, -1.0, latBoundary, child);
//        }else if (gr.low.y == gr.high.y){
//            int middleX = (gr.low.x + gr.high.x)/2;
//            double lonBoundary = (rectangle.high.data[0] + rectangle.low.data[0])/2;
//            Rectangle rectangle0 = new Rectangle(rectangle.low,
//                    new Point(new double[]{lonBoundary, rectangle.high.data[1]}));
//            Rectangle rectangle2 = new Rectangle(new Point(new double[]{lonBoundary, rectangle.low.data[1]}),
//                    rectangle.high);
//            GridNode[] child = new GridNode[4];
//            child[0] = generateGridTree(rectangle0, new GridRectangle(gr.low, new GridPoint(middleX, gr.high.y)));
//            child[1] = null;
//            child[2] = generateGridTree(rectangle2, new GridRectangle(new GridPoint(middleX+1, gr.low.y), gr.high));
//            child[3] = null;
//            return  new GridDirNode(/*rect,*/ gr, lonBoundary, -1.0, child);
//        }else {
//            int middleX = (gr.low.x + gr.high.x)/2;
//            int middleY = (gr.low.y + gr.high.y)/2;
//            double lonBoundary = rectangle.low.data[0] + (rectangle.high.data[0] - rectangle.low.data[0])*(middleX - gr.low.x + 1)/(gr.high.x - gr.low.x + 1);
//            double latBoundary = rectangle.low.data[1] + (rectangle.high.data[1] - rectangle.low.data[1])*(middleY - gr.low.y + 1)/(gr.high.y - gr.low.y + 1);
//            Rectangle rectangle0 = new Rectangle(rectangle.low,
//                    new Point(new double[]{lonBoundary, latBoundary}));
//            Rectangle rectangle1 = new Rectangle(new Point(new double[]{rectangle.low.data[0], latBoundary}),
//                    new Point(new double[]{lonBoundary, rectangle.high.data[1]}));
//            Rectangle rectangle2 = new Rectangle(new Point(new double[]{lonBoundary, rectangle.low.data[1]}),
//                    new Point(new double[]{rectangle.high.data[0], latBoundary}));
//            Rectangle rectangle3 = new Rectangle(new Point(new double[]{lonBoundary, latBoundary}),
//                    rectangle.high);
//            GridNode[] child = new GridNode[4];
//            child[0] = generateGridTree(rectangle0, new GridRectangle(gr.low, new GridPoint(middleX, middleY)));
//            child[1] = generateGridTree(rectangle1, new GridRectangle(new GridPoint(gr.low.x,middleY+1), new GridPoint(middleX, gr.high.y)));
//            child[2] = generateGridTree(rectangle2, new GridRectangle(new GridPoint(middleX+1, gr.low.y), new GridPoint(gr.high.x, middleY)));
//            child[3] = generateGridTree(rectangle3, new GridRectangle(new GridPoint(middleX+1, middleY+1), gr.high));
//            return  new GridDirNode(/*rect,*/ gr, lonBoundary, latBoundary, child);
//        }
//    }
//
//    /**
//     * 获取矩形范围region内某个坐标方向上的索引项密度
//     * @param region 矩形范围
//     * @param axis 0 获取x轴方向的数据密度， 1 获取y轴方向上的数据密度
//     * @return 数据密度
//     */
//    public int[] getElemNumArray(GridRectangle region, int axis) {
//        int[] res;
//        int tmp;
//        if (axis == 0){
//            res = new int[region.high.x - region.low.x + 1];
//            for (int i = region.low.x; i <= region.high.x; i++) {
//                tmp = 0;
//                for (int j = region.low.y; j <= region.high.y; j++)
//                    tmp += data[i][j];
//                res[i - region.low.x] = tmp;
//            }
//        }else {
//            res = new int[region.high.y - region.low.y + 1];
//            for (int i = region.low.y; i <= region.high.y; i++) {
//                tmp = 0;
//                for (int j = region.low.x; j <= region.high.x; j++)
//                    tmp += data[j][i];
//                res[i - region.low.y] = tmp;
//            }
//        }
//        return res;
//   }
//
//    /**
//     * 获取一个矩形内的元素数量
//     */
//    public int getRangeEleNum(GridRectangle range) {
//        int res = 0;
//        for (int i = range.low.x; i <= range.high.x; i++) {
//            for (int j = range.low.y; j <= range.high.y; j++) {
//                res += data[i][j];
//            }
//        }
//        return res;
//    }
//
//    public void alterElemNum(Segment segment, int number) {
//        int row = (int) Math.floor(((segment.data[0] - Constants.globalRegion.low.data[0])/(Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0]))*(Constants.gridDensity+1.0));
//        int col = (int) Math.floor(((segment.data[1] - Constants.globalRegion.low.data[1])/(Constants.globalRegion.high.data[1] - Constants.globalRegion.low.data[1]))*(Constants.gridDensity+1.0));
//        data[row][col] += number;
//    }
//
//    /**
//     * 清空DensityGrid中的data和flag
//     */
//    public void sentDataZero() {
//        for (int i = 0; i < data.length; i++) {
//            for (int j = 0; j < data[i].length; j++) {
//                data[i][j] = 0;
////                flag[i][j] = false;
//            }
//        }
//    }
}
