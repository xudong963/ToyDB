package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    private OpIterator child;
    private int aggFieldNum;
    private int groupFieldNum;
    private Aggregator.Op op;
    private OpIterator[] opIterators;
    private Aggregator aggregator;
    private OpIterator opIterator;
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // some code goes here
        this.child = child;
        aggFieldNum = afield;
        groupFieldNum = gfield;
        op = aop;
        opIterators = new OpIterator[1];
        opIterator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	    return groupFieldNum;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if(groupFieldNum!=-1)
	        return child.getTupleDesc().getFieldName(groupFieldNum);
        return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return aggFieldNum;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	    return child.getTupleDesc().getFieldName(aggFieldNum);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	    return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        Type groupType;
        if(groupFieldNum==-1)
            groupType = null;
        else
            groupType = child.getTupleDesc().getFieldType(groupFieldNum);
        Type aggType = child.getTupleDesc().getFieldType(aggFieldNum);
        if(aggType == Type.INT_TYPE)
            aggregator = new IntegerAggregator(groupFieldNum, groupType, aggFieldNum, op);
        else
            aggregator = new StringAggregator(groupFieldNum, groupType, aggFieldNum, op);

        child.open();
        while (child.hasNext())
        {
            Tuple tuple = child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }
        child.close();
        super.open();
        opIterator = aggregator.iterator();
        opIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	    if (opIterator.hasNext())
        {
            return opIterator.next();
        }
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	    return child.getTupleDesc();
    }

    public void close() {
	    super.close();
	    child.close();
    }

    @Override
    public OpIterator[] getChildren() {
	    opIterators[0] = child;
	    return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    opIterators = children;
    }
}
