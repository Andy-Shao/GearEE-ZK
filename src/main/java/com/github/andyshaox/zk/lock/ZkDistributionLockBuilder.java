package com.github.andyshaox.zk.lock;

import org.apache.zookeeper.ZooKeeper;

import com.github.andyshao.util.EntityOperation;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) Mar 28, 2018<br>
 * Encoding:UNIX UTF-8
 * @author Andy.Shao
 *
 */
@SuppressWarnings("unused")
public final class ZkDistributionLockBuilder {
    private volatile ZooKeeper zk;
    private String lockPath = "/_distribution_lock_node_";
    private int sessionTimeOut = 15000;
    private String connectString = "localhost:2181";
    private String nodeHead = "lock-";
    
    public ZkDistributionLockBuilder nodeHead(String nodeHead) {
        ZkDistributionLockBuilder ret = this.copy();
        ret.nodeHead = nodeHead;
        return ret;
    }
    
    protected ZkDistributionLockBuilder copy() {
        ZkDistributionLockBuilder result = new ZkDistributionLockBuilder();
        EntityOperation.copyProperties(this , result);
        return result;
    }
    
    public ZkDistributionLockBuilder zk(ZooKeeper zk) {
        ZkDistributionLockBuilder ret = this.copy();
        ret.zk = zk;
        return ret;
    }
    
    public ZkDistributionLockBuilder lockPath(String lockPath) {
        ZkDistributionLockBuilder ret = this.copy();
        ret.lockPath = lockPath;
        return ret;
    }
    
    public ZkDistributionLockBuilder sessionTimeOut(int sessionTimeOut) {
        ZkDistributionLockBuilder ret = this.copy();
        ret.sessionTimeOut = sessionTimeOut;
        return ret;
    }
    
    public ZkDistributionLockBuilder connectString(String connectString) {
        ZkDistributionLockBuilder ret = this.copy();
        ret.connectString = connectString;
        return ret;
    }
    
    public ZkDistributionLock build() {
        ZkDistributionLock ret = new ZkDistributionLock();
        EntityOperation.copyProperties(this , ret);
        return ret;
    }
}
