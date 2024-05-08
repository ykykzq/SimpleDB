package simpledb.transaction;

import simpledb.common.Permissions;

public class PageLock {
    private TransactionId transactionId;
    private Permissions permissions;

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public void setPermissions(Permissions permissions) {
        this.permissions = permissions;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public Permissions getPermissions() {
        return permissions;
    }

    public PageLock(TransactionId transactionId, Permissions permissions) {
        this.transactionId = transactionId;
        this.permissions = permissions;
    }
}
