package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId tid;
    private OpIterator opIterator;
    private int tableId;
    private OpIterator[] opIterators;
    private TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        tid = t;
        opIterator = child;
        this.tableId = tableId;
        opIterators = new OpIterator[1];
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();

    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int num = 0;
        while (opIterator.hasNext())
        {
            Tuple tuple = opIterator.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
            num++;
        }
        if(num==0)
            return null;
        Tuple t = new Tuple(getTupleDesc());
        t.setField(0, new IntField(num));
        return t;

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        opIterators[0] = opIterator;
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        opIterators = children;
    }
}
