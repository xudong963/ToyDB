import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class Test
{
    public static void main(String[] args) {
        Integer [] a = {1,3,4,5,6};
        ArrayList<Integer> arrayList = new ArrayList<>(5);
        Collections.addAll(arrayList, a);
        Iterator<Integer> it = arrayList.iterator();
        while (it.hasNext())
        {
            System.out.println(it.next());
        }
    }
}
public class HeapPageIterator implements Iterator<Tuple>
{
    int index = 0;
    @Override
    public boolean hasNext()
    {
        while (index<tuples.length && !isSlotUsed(index))
            index++;

        return index<tuples.length;
    }

    @Override
    public Tuple next() {
        return tuples[index++];
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
    public Iterator<Tuple> iterator() {

        return new HeapPageIterator();
    }






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
    public Page readPage(PageId pid) throws IOException {
        // some code goes here
        int pgSize = BufferPool.getPageSize();
        int pgNum = pid.getPageNumber();
        byte[] data = new byte[pgSize];
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        randomAccessFile.seek(pgNum* pgSize);
        randomAccessFile.write(data);
        return new HeapPage((HeapPageId)pid, data);
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
    public DbFileIterator iterator(TransactionId tid) throws IOException, TransactionAbortedException, DbException {
        // some code goes here
        HeapPage page;
        int tableId = getId();
        HeapPageId pageId = new HeapPageId(tableId, numPages());
        BufferPool bufferPool = new BufferPool(numPages());
        page = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_ONLY );
        Iterator<Tuple> it = page.iterator();
        return (DbFileIterator) it;
    }

}

