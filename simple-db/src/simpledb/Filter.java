package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private OpIterator child;
    private OpIterator[] opIterators;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        predicate = p;
        this.child = child;
        opIterators = new OpIterator[1];
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (child.hasNext()){
            Tuple tuple = child.next();
            if(predicate.filter(tuple))
                return tuple;
        }
        return null;
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
