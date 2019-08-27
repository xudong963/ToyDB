package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc tupleDesc;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgSize = BufferPool.getPageSize();
        int pgNum = pid.getPageNumber();
        byte[] data = new byte[pgSize];
        HeapPage page = null;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(pgNum* pgSize);
            randomAccessFile.read(data);
            page = new HeapPage((HeapPageId)pid, data);
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    /*public class MyDbFileIterator extends AbstractDbFileIterator {

        private int tableId;
        private int pageNum;
        private int pageIndex;
        private TransactionId transactionId;
        private HeapPage.HeapPageIterator heapPageIterator;
        private HeapPage page;
        private HeapPageId pageId;

        public MyDbFileIterator(TransactionId tid)
        {
            tableId = getId();
            pageNum = numPages();
            pageIndex = 0;
            transactionId = tid;
        }
        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if(heapPageIterator==null)
                return null;

            if(heapPageIterator.hasNext())
            {
                pageIndex++;
                return heapPageIterator.next();
            }

            if (pageIndex<pageNum)
            {
                pageIndex++;
                pageId = new HeapPageId(tableId, pageIndex);
                page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
                heapPageIterator = (HeapPage.HeapPageIterator) page.iterator();
                if(heapPageIterator.hasNext())
                {
                    return heapPageIterator.next();
                }
            }
            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if(pageIndex<pageNum)
            {
                pageId = new HeapPageId(tableId, pageIndex);
                page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
                heapPageIterator = (HeapPage.HeapPageIterator) page.iterator();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageIndex = 0;
            pageId = new HeapPageId(tableId, pageIndex);
            page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
            heapPageIterator = (HeapPage.HeapPageIterator) page.iterator();
        }
    }*/
    public class MyDbFileIterator implements DbFileIterator
    {
        private int tableId;
        private int pageNum;
        private int pageIndex;
        private TransactionId transactionId;
        Iterator<Tuple> it;

        public MyDbFileIterator(TransactionId tid)
        {
            transactionId = tid;
            tableId = getId();
            pageNum = numPages();
            it = null;
            pageIndex = -1;
        }

        public Iterator<Tuple> getBeginIt(int pageIndex) throws TransactionAbortedException, DbException {
            PageId pageId = new HeapPageId(tableId, pageIndex);
            return ((HeapPage)Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageIndex = 0;
            it = getBeginIt(pageIndex);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(pageIndex==-1) return false;
            if(!it.hasNext())
            {
                if(pageIndex>=pageNum-1) return false;
                else
                {
                    ++pageIndex;
                    it = getBeginIt(pageIndex);
                }
            }
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!this.hasNext())
                throw new NoSuchElementException();
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pageIndex = -1;
            it = null;
        }
    }
    public DbFileIterator iterator(TransactionId tid)  {
        // some code goes here
        return new MyDbFileIterator(tid);
    }

}
