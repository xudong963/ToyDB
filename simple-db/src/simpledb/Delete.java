package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId tid;
    private OpIterator child;
    private OpIterator[] opIterators;
    private TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
    private boolean deleted;
    public Delete(TransactionId t, OpIterator child) {
        tid = t;
        this.child = child;
        deleted = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(deleted) return null;
        int num = 0;
        while(child.hasNext())
        {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().deleteTuple(tid, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
            num++;
        }
        deleted = true;
        Tuple t = new Tuple(td);
        t.setField(0, new IntField(num));
        return t;
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
