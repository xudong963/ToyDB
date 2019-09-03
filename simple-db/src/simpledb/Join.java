package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate predicate;
    private OpIterator child1;
    private OpIterator child2;
    private OpIterator[] opIterators;
    private Tuple tuple1;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        predicate = p;
        this.child1 = child1;
        this.child2 = child2;
        opIterators = new OpIterator[2];
        tuple1 = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(predicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(predicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        super.open();

    }

    public void close() {
        // some code goes here
        super.close();
        child2.close();
        child1.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //TDD !!!
        while(child1.hasNext())
        {
            if(tuple1==null)
                tuple1 = child1.next();
            while (child2.hasNext())
            {
                Tuple tuple2 = child2.next();
                if(predicate.filter(tuple1, tuple2))
                {
                    TupleDesc tupleDesc = TupleDesc.merge(tuple1.getTupleDesc(), tuple2.getTupleDesc());
                    Tuple tuple = new Tuple(tupleDesc);
                    int numFields1 = tuple1.getTupleDesc().numFields();
                    for(int i=0; i<tupleDesc.numFields(); i++)
                    {
                        if(i<numFields1)
                            tuple.setField(i, tuple1.getField(i));
                        else
                            tuple.setField(i, tuple2.getField(i-numFields1));
                    }
                    return tuple;
                }else
                    break;
            }
            if(!child2.hasNext()){
                child2.rewind();
                tuple1 = null;
            }

        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        opIterators[0] = child1;
        opIterators[1] = child2;
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        opIterators = children;
    }

}
