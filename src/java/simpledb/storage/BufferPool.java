package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private ConcurrentHashMap<PageId, Page> pages;
    private int numPages;

    private EvictionPolicy evictionPolicy;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pages = new ConcurrentHashMap<>();
        this.numPages = numPages;
        this.evictionPolicy = new FIFOEvict();
        this.lockManager = new LockManager();
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
        // some code goes here
        int lockType = perm == Permissions.READ_ONLY ? PageLock.SHARE : PageLock.EXCLUSIVE;
        long startTime = System.currentTimeMillis();
        while(true) {
            try {
                if (lockManager.acquireLock(pid, tid, lockType)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(System.currentTimeMillis() - startTime > 3000) {
                throw new TransactionAbortedException();
            }

        }
//        if(!lockManager.acquireLock(pid, tid, lockType)) {
//            throw new TransactionAbortedException();
//        }

        if (pages.containsKey(pid)) {
            return pages.get(pid);
        }
        if (pages.size() >= numPages) {
            evictPage();
        }
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pages.put(pid, page);
        evictionPolicy.addPage(pid);
        return page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.completeTransaction(tid);
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            restorePage(tid);
        }
        lockManager.completeTransaction(tid);
    }

    private synchronized void restorePage(TransactionId tid) {
        for(PageId pid : pages.keySet()) {
            Page page = pages.get(pid);
            if(page.isDirty() == tid) {
                int tableId = pid.getTableId();
                DbFile table = Database.getCatalog().getDatabaseFile(tableId);
                Page restorePage = table.readPage(pid);
                pages.put(pid, restorePage);
//                evictionPolicy.addPage(pid);
            }
        }
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
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPoll(file.insertTuple(tid, t), tid);
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
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPoll(file.deleteTuple(tid, t), tid);
    }

    public void updateBufferPoll(List<Page> modifiedPages, TransactionId tid) {
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            if(pages.size() > numPages) {
                try {
                    evictPage();
                } catch (DbException e) {
                    e.printStackTrace();
                }
            }
            pages.put(page.getId(), page);
//            evictionPolicy.addPage(page.getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pid : pages.keySet()) {
            try {
                if(pages.get(pid).isDirty() != null)
                    flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page flushPage = pages.get(pid);

        TransactionId dirtier = flushPage.isDirty();
        if(dirtier != null) {
            Database.getLogFile().logWrite(dirtier, flushPage.getBeforeImage(), flushPage);
            Database.getLogFile().force();
//            flushPage.setBeforeImage();
        }

        int tableId = pid.getTableId();
        Database.getCatalog().getDatabaseFile(tableId).writePage(flushPage);
        flushPage.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(PageId pid : pages.keySet()) {
            Page page = pages.get(pid);
            page.setBeforeImage();
            if(page.isDirty() == tid) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId evictPageId = evictionPolicy.getEvictPage();
        PageId firstEvictPageId = evictPageId;
        while(pages.get(evictPageId).isDirty() != null) {
            evictionPolicy.addPage(evictPageId);
            evictPageId = evictionPolicy.getEvictPage();
            if(evictPageId.equals(firstEvictPageId)) {
                throw new DbException("All pages are dirty!");
            }
        }
        try {
            flushPage(evictPageId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        discardPage(evictPageId);
    }

}

interface EvictionPolicy {
    public void addPage(PageId pid);

    public PageId getEvictPage();
}

class FIFOEvict implements EvictionPolicy {
    private final Queue<PageId> queue;

    public FIFOEvict() {
        queue = new ArrayDeque<>();
    }

    @Override
    public void addPage(PageId pid) {
        queue.offer(pid);
    }

    @Override
    public PageId getEvictPage() {
        return queue.poll();
    }
}

class PageLock {
    public static final int SHARE = 0;
    public static final int EXCLUSIVE = 1;
    private TransactionId tid;
    private int lockType;

    public PageLock(TransactionId tid, int lockType) {
        this.tid = tid;
        this.lockType = lockType;
    }

    public TransactionId getTid() {
        return tid;
    }

    public int getLockType() {
        return lockType;
    }

    public void setLockType(int lockType) {
        this.lockType = lockType;
    }
}

class LockManager {
    ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock> > lockMap = new ConcurrentHashMap<>();

    public synchronized boolean acquireLock(PageId pid, TransactionId tid, int lockType) throws InterruptedException, TransactionAbortedException {
        if(!lockMap.containsKey(pid)) {
            PageLock pageLock = new PageLock(tid, lockType);
            ConcurrentHashMap<TransactionId, PageLock> pageLocks = new ConcurrentHashMap<>();
            pageLocks.put(tid, pageLock);
            lockMap.put(pid, pageLocks);
            return true;
        }

        ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pid);
        if(!pageLocks.containsKey(tid)) {
            if(pageLocks.size() > 1) {
                if(lockType == PageLock.EXCLUSIVE) {
                    wait(50);
                    return false;
                }
                else {
                    PageLock pageLock = new PageLock(tid, lockType);
                    pageLocks.put(tid, pageLock);
                    lockMap.put(pid, pageLocks);
                    return true;
                }
            }
            else if(pageLocks.size() == 1) {
                PageLock currentLock = pageLocks.values().iterator().next();
                if(currentLock.getLockType() == PageLock.EXCLUSIVE) {
                    wait(50);
                    return false;
                }
                else {
                    if(lockType == PageLock.EXCLUSIVE) {
                        wait(50);
                        return false;
                    }
                    else {
                        PageLock pageLock = new PageLock(tid, lockType);
                        pageLocks.put(tid, pageLock);
                        lockMap.put(pid, pageLocks);
                        return true;
                    }
                }
            }
        }
        else {
            PageLock currentLock = pageLocks.get(tid);
            if (currentLock.getLockType() == PageLock.EXCLUSIVE) {
                return true;
            } else {
                if (lockType == PageLock.SHARE) {
                    return true;
                } else {
                    if (pageLocks.size() > 1) {
                        wait(50);
                        throw new TransactionAbortedException();
                    } else {
                        PageLock pageLock = new PageLock(tid, lockType);
                        pageLocks.put(tid, pageLock);
                        lockMap.put(pid, pageLocks);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public synchronized boolean releaseLock(PageId pid, TransactionId tid) {
        if(!holdsLock(pid, tid)) {
            return false;
        }
        ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pid);
        pageLocks.remove(tid);
        if(pageLocks.isEmpty()) {
            lockMap.remove(pid);
        }
        this.notifyAll();
        return true;
    }

    public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
        if(!lockMap.containsKey(pid)) {
            return false;
        }

        ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pid);
        if(!pageLocks.containsKey(tid)) {
            return false;
        }

        return true;
    }

    public synchronized void completeTransaction(TransactionId tid) {
        for(PageId pid : lockMap.keySet()) {
            releaseLock(pid, tid);
        }
    }
}
