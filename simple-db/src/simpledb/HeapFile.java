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
        PageId pageId = page.getId();
        int pgSize = BufferPool.getPageSize();
        int pgNum = pageId.getPageNumber();
        byte[] data = page.getPageData();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(pgNum * pgSize);
        randomAccessFile.write(data);
        randomAccessFile.close();
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
        int numPages = numPages();
        ArrayList<Page> arrPages = new ArrayList<>();
        for(int i=0; i<numPages; i++)
        {

            HeapPageId heapPageId = new HeapPageId(getId(), i);   //getId() heapFile Table  because one file one table;
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots()>0)
            {
                heapPage.insertTuple(t);
                arrPages.add(heapPage);
                heapPage.markDirty(true, tid);
                return arrPages;
            }
        }
        HeapPageId newHeapPageId = new HeapPageId(getId(), numPages);
        HeapPage newHeapPage = new HeapPage(newHeapPageId, HeapPage.createEmptyPageData());
        newHeapPage.insertTuple(t);
        arrPages.add(newHeapPage);
        writePage(newHeapPage);

        return arrPages;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> arrPage = new ArrayList<>(1);
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        arrPage.add(page);
        return arrPage;
    }

    // see DbFile.java for javadocs
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
