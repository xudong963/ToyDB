package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe , all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numPages;
    private LockManager lockManager;
    private  ConcurrentHashMap<PageId, Page> pages;

    private class Lock
    {
        private int lockType;
        private TransactionId tid;

        public Lock(int lockType, TransactionId tid)
        {
            this.lockType = lockType;
            this.tid = tid;
        }
    }

    private class LockManager
    {
        private HashMap<PageId, ConcurrentLinkedQueue<Lock>> mapLock;
        // private HashMap<TransactionId, LinkedList<PageId>> tidToPid;
        LockManager()
        {
            mapLock = new HashMap<>();
        }

        public synchronized Boolean acquiredLock(TransactionId tid, PageId pid, int lockType)
        {
            if(mapLock.get(pid) == null || mapLock.get(pid).size()==0)
            {
                mapLock = new HashMap<>();
                ConcurrentLinkedQueue<Lock> locks = new ConcurrentLinkedQueue<>();
                Lock lock = new Lock(lockType, tid);
                locks.add(lock);
                mapLock.put(pid, locks);

                return true;
            } else
            {
                for(Lock lock: mapLock.get(pid))
                {
                    if(lock.tid == tid)
                    {
                        if(lock.lockType == lockType)
                            return true;
                        else
                        {
                            if(lockType == 0)
                                return true;
                            else
                            {
                                //exclusive lock can't coexist with other locks
                                if(mapLock.get(pid).size()==1)
                                {
                                    lock.lockType=1;
                                    return true;
                                }
                                else
                                    return false;
                            }
                        }
                    }
                }
                //now think tid doesn't have any lock
                if(Objects.requireNonNull(mapLock.get(pid).peek()).lockType==1)
                    return false;
                if(lockType == 0)
                {
                    Lock lock = new Lock(0, tid);
                    mapLock.get(pid).add(lock);
                    return true;
                }
            }
            return false;
        }

        public synchronized void releaseLock(PageId pid, TransactionId tid)
        {
            ConcurrentLinkedQueue<Lock> locks = mapLock.get(pid);
            for(Lock lock: locks)
                if(lock.tid == tid)
                    locks.remove(lock);
        }

        public synchronized Boolean holdsLock(TransactionId tid, PageId pid) {
            ConcurrentLinkedQueue<Lock> locks= mapLock.get(pid);
            for(Lock lock : locks) {
                if(lock.tid == tid)
                    return true;
            }
            return false;
        }

        public synchronized Set<PageId> getPid(TransactionId tid) {

            return lockManager.mapLock.keySet();
        }

        public synchronized void commitTransaction(TransactionId tid) {
            Set<PageId> pageIds = getPid(tid);
            for(PageId pid: pageIds) {
                releasePage(tid, pid);
            }
        }
    }
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        lockManager = new LockManager();
        pages = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        int lockType;
        // only-read
        if(perm == Permissions.READ_ONLY)
            lockType = 0;
        else
            lockType = 1;
        long start = System.currentTimeMillis();
        long timeOut = new Random().nextInt(2000) + 1000;
        boolean isSucc = false;
        while (!isSucc) {
            long now = System.currentTimeMillis();
            if (now - start > timeOut)
                throw new TransactionAbortedException();
            isSucc = lockManager.acquiredLock(tid, pid, lockType);
        }
        if(pages.get(pid)!=null)
        {
            return pages.get(pid);
        }else
        {
            int tableId = pid.getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tableId);
            Page page = file.readPage(pid);
            if(numPages==pages.size())
                evictPage();
            pages.put(pid, page);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        for(PageId pid: lockManager.getPid(tid)) {
            if(commit) {
                flushPage(pid);
            }else {
                Page page = pages.get(pid);
                if(page!=null && page.isDirty()==tid) {
                    discardPage(pid);
                }
            }
        }
        lockManager.commitTransaction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> ArrPages = file.insertTuple(tid, t);
        for(Page page: ArrPages)
        {
            page.markDirty(true, tid);
            pages.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> arrPages = file.deleteTuple(tid, t);
        for(Page p: arrPages)
        {
            p.markDirty(true, tid);
            pages.put(p.getId(), p);
        }
        
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(PageId pid: pages.keySet())
            flushPage(pid);

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (!pages.containsKey(pid)) return;
        Page page = pages.get(pid);
        if(page.isDirty()!=null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            //HeapFile heapFile = new HeapFile((File) file, file.getTupleDesc());
            file.writePage(page);
            page.markDirty(false, null);
            pages.remove(pid);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        for(PageId pid : pages.keySet()) {
            //NO STEAL
            if(pages.get(pid).isDirty()==null) {
                discardPage(pid);
                return;
            }
        }
        throw new DbException("all pages marked dirty");
    }
}
