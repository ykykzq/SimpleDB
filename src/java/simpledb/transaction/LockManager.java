package simpledb.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManager {
    private ConcurrentHashMap<PageId, List<PageLock>>pageLocks;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pageId, Permissions permissions){
        String threadName = Thread.currentThread().getName();
        List<PageLock> locks = pageLocks.get(pageId);
        final PageLock pageLock = new PageLock(tid, permissions);
        // 如果页面没有上锁，将其锁添加进去
        if(pageLocks.size() == 0 || locks == null){
            if(locks==null){
                locks = new ArrayList<>();
            }
            locks.add(pageLock);
            pageLocks.put(pageId,locks);
            return true;
        }
        if(locks.size()==1){
            final PageLock fLock = locks.get(0);
            // 锁升级
            if(fLock.getTransactionId().equals(tid)){
                if(fLock.getPermissions().equals(Permissions.READ_ONLY)&&pageLock.getPermissions().equals(Permissions.READ_WRITE)){
                    fLock.setPermissions(Permissions.READ_WRITE);
                }
                return true;
            }
            else{
                if(fLock.getPermissions().equals(Permissions.READ_ONLY)&&pageLock.getPermissions().equals(Permissions.READ_ONLY)){
                    //共享锁
                    locks.add(pageLock);
                    return true;
                }else{
                    // 必须等待读锁退出
                    return false;
                }
            }
        }
        if(pageLock.getPermissions().equals(Permissions.READ_WRITE)){
            return false;
        }
        for (PageLock lock : locks) {
            final TransactionId ttid = lock.getTransactionId();
            if(ttid!=null&&ttid.equals(tid)){
                return true;
            }
        }
        locks.add(pageLock);
        return true;
    }
    public synchronized void releaseLock(TransactionId tid,PageId pageId){
        final List<PageLock> pageLocks = this.pageLocks.get(pageId);
        for (PageLock lock : pageLocks) {
            final TransactionId ttid = lock.getTransactionId();
            if(ttid!=null&&ttid.equals(tid)){
                pageLocks.remove(lock);
                if(pageLocks.isEmpty()){
                    this.pageLocks.remove(pageId);
                }
                return;
            }
        }
    }
    public synchronized void releaseAllLock(TransactionId transactionId){
        for (PageId pid : pageLocks.keySet()) {
            final List<PageLock> locks = this.pageLocks.get(pid);
            for (PageLock lock : locks) {
                if(transactionId.equals(lock.getTransactionId())){
                    locks.remove(lock);
                    if(locks.isEmpty()){
                        pageLocks.remove(pid);
                    }
                    break;
                }
            }
        }
    }
    public synchronized boolean holdsLock(TransactionId tid,PageId pid){
        final List<PageLock> locks = this.pageLocks.get(pid);
        if(locks==null)return false;
        for (PageLock lock : locks) {
            final TransactionId ttid = lock.getTransactionId();
            if(ttid!=null&&ttid.equals(tid))return true;
        }
        return false;
    }
}
