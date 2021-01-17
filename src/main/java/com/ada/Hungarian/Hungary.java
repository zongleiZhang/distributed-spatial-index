package com.ada.Hungarian;

import java.util.*;

public class Hungary {

    private int n;                    //维数
    private int[][] s;        //原始矩阵
    private int[][] p;        //归约矩阵
    private int[][] q;        //0:未被画线 1:画了1次 2: 画了2次(交点)

    private int[] x;
    private int[] y;       //画线时是否被打勾，1是0不是
    private boolean need_stack;//当为true时，需要进栈
    private Stack<Main_Node> stack1;
    private Random random;

    private void init(int[][] valus){
        n = valus.length;
        s = valus;
        p = new int[n][n];
        q = new int[n][n];
        x = new int[n];
        y = new int[n];
        need_stack = false;
        stack1 = new Stack<>();
        random = new Random(123456789L);
    }

    private void close(){
        s = null;
        p = null;
        q = null;
        x = null;
        y = null;
        stack1 = null;
    }

    public int[][] calculate(int[][] valus) {
        init(valus);
        for(int i=0;i<valus.length;i++) {
            for (int j = 0; j < valus[i].length; j++) {
                s[i][j] = valus[i][j];
                p[i][j] = s[i][j];
            }
        }
        //第一步：变出零来
        for(int i=0;i<n;i++) {
            int min = p[i][0];
            for (int j = 1; j < n; j++)
                min = Math.min(min, p[i][j]);
            if (min != 0) {
                for (int j = 0; j < n; j++)
                    p[i][j] -= min;
            }
        }

        for(int j=0;j<n;j++) {
            int min = p[0][j];
            for (int i = 1; i < n; i++)
                min = Math.min(min, p[i][j]);
            if (min != 0) {
                for (int i = 0; i < n; i++)
                    p[i][j] -= min;
            }
        }
        //第二步find()
        int t=n;//t是本次find()要找的个数
        //在第二步里是n,第三步回退的是top+1
        //第四步还是n
        while(find()<t) {
//            t=top+1;//进栈的元素个数,也是出栈的元素个数\
            t = stack1.size();
            //第三步drawLine
            if (drawLine() == n)//将元素退栈，返回第二步
            {
                while (!stack1.isEmpty()/*top!=-1*/) {
                    Main_Node aaa = stack1.pop(); //stack[top--];
                    p[aaa.row][aaa.col] = 0;
                }
                continue;
            }

            //第四步
            //最小的未被划线的数
            int min = Integer.MAX_VALUE;
            for (int i = 0; i < n; ++i)
                for (int j = 0; j < n; ++j)
                    if (q[i][j] == 0 && min > p[i][j])
                        min = p[i][j];

            //更新未被划到的和交点
            for (int i = 0; i < n; ++i)
                for (int j = 0; j < n; ++j)
                    if (q[i][j] == 0)
                        p[i][j] -= min;
                    else if (q[i][j] == 2)
                        p[i][j] += min;
            //恢复-1、-2为0   ,   t=n
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    if (p[i][j] < 0) {
                        p[i][j] = 0;
                    }
                }
            }
            t = n;
        }

        //求和及输出
        int[][] res = new int[n][2];
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j)
                if (p[i][j] == -2) {
                    res[i][0] = i;
                    res[i][1] = j;
                }
        }
        close();
        Set<Integer> check = new HashSet<>();
        for (int[] re : res) {
            if (check.contains(re[1]))
                throw new IllegalArgumentException("Hungary error.");
            else
                check.add(re[1]);
        }
        return res;
    }


    /**
     * 记录每行每列0的个数
     */
    public void countZero(int []row,int[]col) {
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j)
                if (p[i][j] == 0) {
                    row[i]++;
                    col[j]++;
                }

        }
    }

    //画最少的线覆盖所有0元素
    public int  drawLine() {
        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                q[i][j] = 0;

        for (int i = 0; i < n; ++i)
            x[i] = 1;
        for (int j = 0; j < n; ++j)
            y[j] = 0;

        //row 对所有不含独立0元素的行打勾！
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                if (p[i][j] == -2) {
                    x[i] = 0;

                    break;
                }
            }
        }

        boolean is = true;
        while (is) {  //循环直到没有勾可以打
            is = false;
            //col 对打勾的行中含φ元素的未打勾的列打勾
            for (int i = 0; i < n; ++i) {
                if (x[i] == 1) {
                    for (int j = 0; j < n; ++j) {
                        if (p[i][j] == -1 && y[j] == 0) {
                            y[j] = 1;
                            is = true;
                        }
                    }
                }
            }

            //row 对打勾的列中含◎的未打勾的行打勾
            for (int j = 0; j < n; ++j) {
                if (y[j] == 1) {
                    for (int i = 0; i < n; ++i) {
                        if (p[i][j] == -2 && x[i] == 0) {
                            x[i] = 1;
                            is = true;
                        }
                    }
                }
            }
        }

        //没有打勾的行和有打勾的列画线，这就是覆盖所有0元素的最少直线数
        int line = 0;
        for (int i = 0; i < n; ++i) {
            if (x[i] == 0) {
                for (int j = 0; j < n; ++j)
                    q[i][j]++;
                line++;
            }

            if (y[i] == 1) {
                for (int j = 0; j < n; ++j)
                    q[j][i]++;
                line++;
            }
        }
        return line;
    }

    //更新行列的0且进栈
    public void refresh1(int index,int index2,int[]row,int[]col) {
        for (int j = 0; j < n; ++j)
            if (p[index][j] == 0){ // 若该行还有0且没被划掉才更新
                row[index]--;
                col[j]--;
                p[index][j] = -1;
                stack1.push(new Main_Node(index, j));
            }
        for (int i = 0; i < n; ++i) {
            if (p[i][index2] == 0) {
                row[i]--;
                col[index2]--;
                p[i][index2] = -1;
                stack1.push(new Main_Node(i, index2));
            }
        }
    }

    //更新行列的0不进栈
    public void refresh2(int index,int index2,int[]row,int[]col) {
        for (int j = 0; j < n; ++j) {
            if (p[index][j] == 0) {//若该行还有0且没被划掉才更新
                row[index]--;
                col[j]--;
                p[index][j] = -1;
            }
        }
        for (int i = 0; i < n; ++i){
            if (p[i][index2] == 0) {
                row[i]--;
                col[index2]--;
                p[i][index2] = -1;
            }
        }
    }

    /**
     * 第二步、
     * 找独立0元素个数
     * 1.找含0最少的那一行/列
     * 2.划掉，更新该行/列0元素所在位置的row[],col[]
     * 3.直到所有0被划线退出
     * 4.need为false说明只做了前两步，need为true说明做了第四步（第四步是猜的，所以要进栈，如果第三步发现猜错了，出栈）
     */
    public int find() {
        int zero = 0;     //独立0元素的个数
        int[] row = new int[n];
        int[] col = new int[n];//行列0元素个数
        countZero(row, col);
        while (true) {
            for (int i = 0; i < n; i++) {
                if (row[i] == 0)
                    row[i] = Integer.MAX_VALUE;
                if (col[i] == 0)
                    col[i] = Integer.MAX_VALUE;
            }
            boolean stop = true;
            int row_min = Arrays.stream(row).min().getAsInt();
            int col_min = Arrays.stream(col).min().getAsInt();
            if (row_min == Integer.MAX_VALUE) break;
            if (row_min <= col_min) {  // 行有最少0元素
                if (row_min > 1)
                    need_stack = true;
                //找含0最少的那一行
                int tmp = Integer.MAX_VALUE;
                int index = -1;
                for (int i = 0; i < n; ++i) {
                    if (tmp > row[i]) {
                        tmp = row[i];
                        index = i;
                    }
                }

                /*找该行任意一个没被划掉的0元素(独立0元素)，任意找一个*/
                int index2 = -1;            //该行独立0元素的列值
                int count = (int) (random.nextDouble() * row[index]);//随机选哪一个0
                int k = 0;
                for (int i = 0; i < n; ++i) {
                    if (p[index][i] == 0) {
                        if (k == count) {
                            index2 = i;
                            stop = false;            //找到独立0元素则继续循环
                            zero++;                //独立0元素的个数
                            break;
                        }
                        k++;
                    }
                }

                //找不到独立0元素了
                if (stop)
                    break;

                //标记
                row[index]--;
                col[index2]--;
                p[index][index2] = -2;//独立0元素
                if (need_stack) {
                    stack1.push(new Main_Node(index, index2));
                    refresh1(index, index2, row, col);//更新其他0且都要进栈
                } else
                    refresh2(index, index2, row, col);
            } else       //列有最少0元素
            {
                if (col_min > 1) {
                    need_stack = true;
                }

                int tmp = Integer.MAX_VALUE;
                int index = -1;
                for (int i = 0; i < n; ++i) {
                    if (tmp > col[i]) {
                        tmp = col[i];
                        index = i;
                    }
                }

                int index2 = -1;
                int count = (int) (random.nextDouble() * col[index]);//随机选哪一个0
                int k = 0;
                for (int i = 0; i < n; ++i) {
                    if (p[i][index] == 0) {
                        if (k == count) {
                            index2 = i;
                            stop = false;            //找到独立0元素则继续循环
                            zero++;                //独立0元素的个数
                            break;
                        }
                        k++;
                    }
                }
                if (stop)
                    break;

                row[index2]--;
                col[index]--;
                p[index2][index] = -2;
                if (need_stack) {
                    stack1.push(new Main_Node(index2, index));
                    refresh1(index2, index, row, col);//更新其他0且都要进栈
                } else
                    refresh2(index2, index, row, col);
            }
        }
        return zero;
    }
}
