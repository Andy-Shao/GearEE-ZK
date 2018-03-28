package com.github.andyshaox.zk.lock;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.github.andyshao.lock.DistributionLock;
import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.LockException;
import com.github.andyshaox.zk.utils.ZooKeepers;

import lombok.AccessLevel;
import lombok.Setter;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

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
    private volatile String nodeName;
    @Setter(value = AccessLevel.PACKAGE)
    private volatile ZooKeeper zk;
    @Setter(value = AccessLevel.PACKAGE)
    private String lockPath;
    @Setter(value = AccessLevel.PACKAGE)
    private int sessionTimeOut;
    @Setter(value = AccessLevel.PACKAGE)
    private String connectString;
    @Setter(value = AccessLevel.PACKAGE)
    private String nodeHead;
    
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
    
    protected boolean tryAcquireLock(final ExpireMode mode , int times) throws InterruptedException {
        Long l = new Date().getTime();
        switch(mode) {
        case MILISECONDS:
            l = l + times;
            break;
        case SECONDS:
            l = l + (times * 1000);
            break;
                
        case IGNORE:
            l = Long.MAX_VALUE;
        default :
            break;
        }
        if(this.lockOwner.isOwner()) {
            this.lockOwner.setTimeSign(l);
            return this.lockOwner.increment();
        }
        boolean hasLock = false;
        try {
            if(zk.exists(nodeName , false) == null) nodeName = zk.create(fullNodePath() , "".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL);
            Optional<String> lockOwnerName = zk.getChildren(lockPath , false).stream().sorted().findFirst();
            if(Objects.equals(nodeName , lockOwnerName.get())) hasLock = true;
            else hasLock = false;
        } catch (KeeperException e) {
            throw new LockException(e);
        }
        if(hasLock) {
            this.lockOwner.setTimeSign(l);
            this.lockOwner.increment();
        }
        return hasLock;
    }
    
    protected boolean acquireLock(final ExpireMode mode , int times) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Long l = new Date().getTime();
        switch(mode) {
        case MILISECONDS:
            l = l + times;
            break;
        case SECONDS:
            l = l + (times * 1000);
            break;
            
        case IGNORE:
            l = Long.MAX_VALUE;
        default :
            break;
        }
        if(this.lockOwner.isOwner()) {
            this.lockOwner.setTimeSign(l);
            return this.lockOwner.increment();
        }
        final AtomicBoolean hasLock = new AtomicBoolean(false);
        final AtomicBoolean hasReturn = new AtomicBoolean(false);
        try {
            if(zk.exists(String.format("%s/%s" , lockPath, nodeName) , false) == null) nodeName = zk.create(fullNodePath() , "".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL);
            waitLock(countDownLatch , hasLock, hasReturn);
        } catch (KeeperException e) {
            throw new LockException(e);
        }
        if(hasLock.get()) {
            this.lockOwner.setTimeSign(l);
            this.lockOwner.increment();
        }
        switch (mode) {
        case MILISECONDS:
            countDownLatch.await(times , TimeUnit.MILLISECONDS);
            break;
        case SECONDS:
            countDownLatch.await(times , TimeUnit.SECONDS);
            break;
        default:
            countDownLatch.await();
            break;
        }
        hasReturn.set(true);
        return hasLock.get();
    }

    protected void waitLock(final CountDownLatch countDownLatch , final AtomicBoolean hasLock, final AtomicBoolean hasReturn) throws KeeperException , InterruptedException {
        Optional<String> lockOwnerName = zk.getChildren(lockPath , false).stream().sorted().findFirst();
        if(Objects.equals(nodeName , lockOwnerName.get())) {
            hasLock.set(true);
            countDownLatch.countDown();
        } else {
            hasLock.set(false);
            zk.exists(String.format("%s/%s" , lockPath, lockOwnerName.get()) , watch -> {
                EventType type = watch.getType();
                switch (type) {
                case NodeDeleted:
                    if(countDownLatch.getCount() > 0 && !hasReturn.get()) try {
                        waitLock(countDownLatch, hasLock, hasReturn);
                    } catch (KeeperException | InterruptedException e) {
                        throw new LockException(e);
                    }
                    countDownLatch.countDown();
                    break;

                default:
                    break;
                }
            });
        }
    }
    
    protected String fullNodePath() {
        return String.format("%s/%s" , lockPath, nodeHead);
    }

    protected synchronized void connectZk() throws InterruptedException {
        if(zk != null) return;
        try {
            zk = ZooKeepers.connect(connectString , sessionTimeOut);
        } catch (IOException e) {
            throw new LockException(e);
        }
    }
    
    @Override
    public void unlock() {
        tryUnlock();
    }
    
    public synchronized boolean tryUnlock() {
        if(this.lockOwner.canUnlock()) {
            try {
                zk.delete(String.format("%s/%s" , lockPath, nodeName) , -1);
            } catch (InterruptedException | KeeperException e1) {
                throw new LockException(e1);
            }
            return true;
        }
        return false;
    }

    @Override
    public void lock() {
        lock(ExpireMode.IGNORE, -1);
    }

    @Override
    public void lock(ExpireMode mode , int times) {
        //TODO has bugs
        while(true) {
            try {
                connectZk();
                createPathes();
                acquireLock(mode , times);
                return;
            } catch (InterruptedException e) {}
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lockInterruptibly(ExpireMode.IGNORE , -1);
    }

    @Override
    public void lockInterruptibly(ExpireMode mode , int times) throws InterruptedException {
        connectZk();
        createPathes();
        acquireLock(mode , times);
    }

    @Override
    public boolean tryLock() {
        return tryLock(ExpireMode.IGNORE, -1);
    }

    @Override
    public boolean tryLock(ExpireMode mode , int times) {
        
        //TODO has bugs
        try {
            connectZk();
            createPathes();
            return tryAcquireLock(mode , times);
        } catch (InterruptedException e) {
            return false;
        }
    }

    private void createPathes() throws InterruptedException {
        try {
            ZooKeepers.createPathRecursively(zk , lockPath);
        } catch (KeeperException e) {
            throw new LockException(e);
        }
    }
}
