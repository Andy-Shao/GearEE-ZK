package com.github.andyshaox.zk.lock;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.ZooKeeper;

import com.github.andyshao.lock.DistributionLock;
import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.LockException;
import com.github.andyshaox.zk.utils.ZooKeepers;

import lombok.AccessLevel;
import lombok.Setter;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) Mar 28, 2018<br>
 * Encoding: UNIX UTF-8
 * @author andy.shao
 *
 */
public class ZkDistributionLock implements DistributionLock {
    private volatile LockOwer lockOwner = this.new LockOwer();
    @Setter(value = AccessLevel.PACKAGE)
    private volatile ZooKeeper zk;
    @Setter(value = AccessLevel.PACKAGE)
    private String lockPath;
    @Setter(value = AccessLevel.PACKAGE)
    private int sessionTimeOut;
    @Setter(value = AccessLevel.PACKAGE)
    private String connectString;
    
    private class LockOwer {
        private volatile Thread thread;
        private volatile AtomicLong size;
        private volatile long timeSign = 0;
        
        public void setTimeSign(long timeSign) {
            this.timeSign = timeSign;
        }

        public synchronized boolean isOwner() {
                return Objects.equals(this.thread, Thread.currentThread());
        }
        
        public synchronized boolean increment() {
            if(thread == null) {
                this.thread = Thread.currentThread();
                this.size = new AtomicLong(0L);
                return false;
            } else {
                this.size.incrementAndGet();
                return true;
            }
        }
        
        public synchronized boolean canUnlock() {
                if(this.timeSign <= new Date().getTime()) {
                    this.thread = null;
                    this.size = null;
                    return false;
                } else if(Objects.equals(Thread.currentThread() , this.thread)) {
                if(this.size.longValue() <= 0L) {
                    this.thread = null;
                    this.size = null;
                    return true;
                } else {
                    this.size.decrementAndGet();
                    return false;
                }
            } else return false;
        }
    }
    
    protected synchronized boolean tryLock01(ExpireMode mode , int times) throws InterruptedException {
        connectZk();
        return false;
    }

    protected void connectZk() throws InterruptedException {
        if(zk != null) return;
        try {
            zk = ZooKeepers.connect(connectString , sessionTimeOut);
        } catch (IOException e) {
            throw new LockException(e);
        }
    }
    
    @Override
    public synchronized void unlock() {
        if(this.lockOwner.canUnlock()) {
            if(zk != null) try {
                zk.close();
            } catch (InterruptedException e) {
                throw new LockException(e);
            }
            zk = null;
        }
    }

    @Override
    public void lock() {
        lock(ExpireMode.IGNORE, -1);
    }

    @Override
    public void lock(ExpireMode mode , int times) {
        // TODO Auto-generated method stub

    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lockInterruptibly(ExpireMode.IGNORE , -1);
    }

    @Override
    public void lockInterruptibly(ExpireMode mode , int times) throws InterruptedException {
        
    }

    @Override
    public boolean tryLock() {
        return tryLock(ExpireMode.IGNORE, -1);
    }

    @Override
    public boolean tryLock(ExpireMode mode , int times) {
        try {
            return tryLock01(mode , times);
        } catch (InterruptedException e) {
            return false;
        }
    }
}
