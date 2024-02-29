package com.github.andyshaox.zk.lock;

import com.github.andyshao.lock.DistributionLock;
import com.github.andyshao.lock.ExpireMode;
import com.github.andyshao.lock.LockException;
import com.github.andyshaox.zk.utils.ZooKeepers;
import com.google.common.base.Splitter;
import lombok.AccessLevel;
import lombok.Setter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    private final ThreadLocal<String> nodeName = new ThreadLocal<>();
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
    
    protected boolean tryAcquireLock(final ExpireMode mode, final int times, final Long timeSign) throws InterruptedException {
        if(this.lockOwner.isOwner()) {
            this.lockOwner.setTimeSign(timeSign);
            return this.lockOwner.increment();
        }
        final AtomicBoolean hasLock = new AtomicBoolean(false);
        try {
            register();
            Optional<String> lockOwnerName = zk.getChildren(lockPath , false).stream().sorted().findFirst();
            if(Objects.equals(nodeName.get() , lockOwnerName.get())) hasLock.set(true);
            else hasLock.set(false);;
        } catch (KeeperException e) {
            throw new LockException(e);
        }
        if(hasLock.get()) {
            this.lockOwner.setTimeSign(timeSign);
            this.lockOwner.increment();
        }
        return hasLock.get();
    }
    
    protected boolean acquireLock(final ExpireMode mode , int times, Long timeSign) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        if(this.lockOwner.isOwner()) {
            this.lockOwner.setTimeSign(timeSign);
            return this.lockOwner.increment();
        }
        final AtomicBoolean hasLock = new AtomicBoolean(false);
        final AtomicBoolean hasReturn = new AtomicBoolean(false);
        try {
            register();
            waitLock(countDownLatch , hasLock, hasReturn);
        } catch (KeeperException e) {
            throw new LockException(e);
        }
        switch (mode) {
            case MILLISECONDS:
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
        if(hasLock.get()) {
            this.lockOwner.setTimeSign(timeSign);
            this.lockOwner.increment();
        }
        return hasLock.get();
    }

    protected Long calculateTimeSign(final ExpireMode mode , int times) {
        Long l = new Date().getTime();
        switch(mode) {
            case MILLISECONDS:
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
        return l;
    }

    protected void register() throws KeeperException , InterruptedException {
        if(nodeName.get() == null) {
            synchronized (this) {
                if(nodeName.get() == null || (zk.exists(String.format("%s/%s" , lockPath, nodeName.get()) , false) == null)) {
                    String fullPath = zk.create(fullNodePath() , "".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL);
                    List<String> pathes = Splitter.on('/').splitToList(fullPath);
                    nodeName.set(pathes.get(pathes.size() - 1));
                }
            }
        }
    }

    protected void waitLock(final CountDownLatch countDownLatch , final AtomicBoolean hasLock, final AtomicBoolean hasReturn) throws KeeperException , InterruptedException {
        Optional<String> lockOwnerName = zk.getChildren(lockPath , false).stream().sorted().findFirst();
        if(Objects.equals(nodeName.get() , lockOwnerName.get())) {
            hasLock.set(true);
            countDownLatch.countDown();
        } else {
            hasLock.set(false);
            final String nodeNm = this.nodeName.get();
            zk.exists(String.format("%s/%s" , lockPath, lockOwnerName.get()) , watch -> {
                nodeName.set(nodeNm);
                EventType type = watch.getType();
                switch (type) {
                case NodeDeleted:
                    if(countDownLatch.getCount() > 0 && !hasReturn.get()) try {
                        waitLock(countDownLatch, hasLock, hasReturn);
                    } catch (KeeperException | InterruptedException e) {
                        throw new LockException(e);
                    }
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
                zk.delete(String.format("%s/%s" , lockPath, nodeName.get()) , -1);
                nodeName.set(null);
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
        final Long timeSign = calculateTimeSign(mode , times);
        while(new Date().getTime() < timeSign) {
            try {
                connectZk();
                createPathes();
                acquireLock(mode , times, timeSign);
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
        acquireLock(mode , times, calculateTimeSign(mode , times));
    }

    @Override
    public boolean tryLock() {
        return tryLock(ExpireMode.IGNORE, -1);
    }

    @Override
    public boolean tryLock(ExpireMode mode , int times) {
        final Long timeSign = calculateTimeSign(mode , times);
        while(new Date().getTime() < timeSign) {
            try {
                connectZk();
                createPathes();
                return tryAcquireLock(mode , times, timeSign);
            } catch (InterruptedException e) {
                continue;
            }
        }
        return false;
    }

    private void createPathes() throws InterruptedException {
        try {
            ZooKeepers.createPathRecursively(zk , lockPath);
        } catch (KeeperException e) {
            throw new LockException(e);
        }
    }
}
