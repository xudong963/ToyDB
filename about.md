### ToyDB 总结

(2019.11.20)

```sql
select * from course;
+-----------+----------------------------+------------+---------+
| course_id | title                      | dept_name  | credits |
+-----------+----------------------------+------------+---------+
| BIO-101   | Intro. to Biology          | Biology    |       4 |
| BIO-301   | Genetics                   | Biology    |       4 |
| BIO-399   | Computational Biology      | Biology    |       3 |
| CS-101    | Intro. to Computer Science | Comp. Sci. |       4 |
| CS-190    | Game Design                | Comp. Sci. |       4 |
| CS-315    | Robotics                   | Comp. Sci. |       3 |
| CS-319    | Image Processing           | Comp. Sci. |       3 |
| CS-347    | Database System Concepts   | Comp. Sci. |       3 |
| EE-181    | Intro. to Digital Systems  | Elec. Eng. |       3 |
| FIN-201   | Investment Banking         | Finance    |       3 |
| HIS-351   | World History              | History    |       3 |
| MU-199    | Music Video Production     | Music      |       3 |
| PHY-101   | Physical Principles        | Physics    |       4 |
+-----------+----------------------------+------------+---------+
13 rows in set (0.09 sec)

select * from student;
+-------+----------+------------+----------+
| ID    | name     | dept_name  | tot_cred |
+-------+----------+------------+----------+
| 00128 | Zhang    | Comp. Sci. |      102 |
| 12345 | Shankar  | Comp. Sci. |       32 |
| 19991 | Brandt   | History    |       80 |
| 23121 | Chavez   | Finance    |      110 |
| 44553 | Peltier  | Physics    |       56 |
| 45678 | Levy     | Physics    |       46 |
| 54321 | Williams | Comp. Sci. |       54 |
| 55739 | Sanchez  | Music      |       38 |
| 70557 | Snow     | Physics    |        0 |
| 76543 | Brown    | Comp. Sci. |       58 |
| 76653 | Aoi      | Elec. Eng. |       60 |
| 98765 | Bourikas | Elec. Eng. |       98 |
| 98988 | Tanaka   | Biology    |      120 |
+-------+----------+------------+----------+
13 rows in set (0.46 sec)
```

下面这条 SQL 查询包含了基本的操作

```sql
select course.dept_name, count(*), avg(course.credits)
    from course left outer join student on course.dept_name=student.dept_name
    group by course.dept_name;
```

**1**. **Parser** (不熟悉)

- 采用 **Zql** : 一个由 java 编写的 **SQL parser**

**2**. **Logical plan** 
```java
public LogicalPlan() {
    joins = new Vector<LogicalJoinNode>();
    filters = new Vector<LogicalFilterNode>();
    tables = new Vector<LogicalScanNode>();
    subplanMap = new HashMap<String, OpIterator>();
    tableMap = new HashMap<String,Integer>();

    selectList = new Vector<LogicalSelectListNode>();
    this.query = "";
}
```

- **Logical plan** 生成 **logical query plan**, 被用于优化, 它包含 scan nodes, join nodes, filter nodes, a select list, and a group by field.

- **LogicalFilterNode** : 对应 **Where** 子句, **Filter : t.f p c**, 其中 **t** 是 **table**, **p** 是 **predicate**, **c** 是常量

- **JoinNode**

    - **LogicalJoinNode** : 对应 **join** 操作, **table1.joinField1 op table2.joinField2**
    - **LogicalSubplanJoinNode** : join 一个表和一个子查询

- **LogicalScanNode** : 对应 **from** 子句

- **LogicalSelectListNode** : 对应 **select** 子句, 即 **projection**, 可能包含 **aggregation operation**

**3. Physical plan**



**4. 物理优化(基于代价的优化)**

- 同一个逻辑算子可能因为**数据读取, 计算方式**等不同会产生多个不同的物理算子, 例如逻辑上的 Join 算子转换成物理算子可以选择 HashJoin、SortMergeJoin、IndexLookupJoin

- 基于代价优化的的主要思路是计算所有可能的执行计划的代价，并挑选代价最小的执行计划的路径, 首先采集对应表的**统计信息**( **tableStat** ), 用来计算出**每个算子的代价**

- **统计信息** : 
    - **histogram**
    - **ScanCost estimation**: npages * ioCostPerPage
    - **TableCardinality estimation**: ntups * selectivityFactor
    - 利用直方图进行 **Selectivity estimation**

- **Join 重排** : ToyDB 对 join 进行了重排, join 的顺序影响中间结果的数据量, 而且也会影响具体的 join 算法. 采用动态规划保留所有可能的 **join** 顺序, 然后遍历找出最小代价最小的

- **Join 算法** : ToyDB 使用了 **IndexLookupJoin**, 性能是很低的

- **select, aggregate 和 group by** 

    - ToyDB 只支持对一列 **aggregate** 和 **group by**
    - **aggregation** 采用了 **StreamAgg**


**5**. **Execute plan**

**6**. -----

**7. Locking** : 在 **BufferPool** 中使用了 **two phase locking**, **S-LOCK 和 X-LOCK**

**8. Transactions**: 数据库系统的一组动作, 包括 插入, 删除, 读写等. **ACID**

**9. HeapFile, HeapPage, BufferPool** : HeapFile 提供一种方法从磁盘读写数据(没有使用 **B+树**), HeapFile 中的 page 是 **slots** 的集合, 每一个 **slot** 都包含一个 **tuple**, 每一个 page 还包含一个由 **bitmap** 组成的 **header**. **BufferPool** 在内存中缓存从磁盘中读取的 **page**


**不足**

- 没有进行**逻辑优化**
- Join 算法采用 **IndexLookupJoin** 效率很低
- 不支持 **redo undo**

