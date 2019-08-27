package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private HeapFile.MyDbFileIterator myDbFileIterator;
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        myDbFileIterator = (HeapFile.MyDbFileIterator) Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        myDbFileIterator = (HeapFile.MyDbFileIterator) Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        myDbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        Type []typeArr = new Type[tupleDesc.numFields()];
        String []fieldName = new String[tupleDesc.numFields()];
        for(int i=0; i<tupleDesc.numFields(); i++)
        {
            if(tableAlias!=null)
            {
                if(tupleDesc.getFieldName(i)==null)
                {
                    fieldName[i]  = tableAlias+".null";
                    typeArr[i] = tupleDesc.getFieldType(i);
                }
                else
                {
                    fieldName[i] = tableAlias+"."+tupleDesc.getFieldName(i);
                    typeArr[i] = tupleDesc.getFieldType(i);
                }
            }else
            {
                if(tupleDesc.getFieldName(i)==null)
                {
                    fieldName[i]  = "null.null";
                    typeArr[i] = tupleDesc.getFieldType(i);
                }
                else
                {
                    fieldName[i] = "null."+tupleDesc.getFieldName(i);
                    typeArr[i] = tupleDesc.getFieldType(i);
                }
            }
        }
        return new TupleDesc(typeArr, fieldName);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return myDbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return myDbFileIterator.next();
    }

    public void close() {
        // some code goes here
        myDbFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        myDbFileIterator.rewind();
    }
}
