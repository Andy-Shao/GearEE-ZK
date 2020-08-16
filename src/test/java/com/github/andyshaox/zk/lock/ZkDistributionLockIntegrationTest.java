package com.github.andyshaox.zk.lock;

import com.github.andyshao.lock.ExpireMode;
import com.github.andyshaox.zk.utils.ZooKeepers;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZkDistributionLockIntegrationTest {
    public static void testTryLock() throws InterruptedException, IOException {
        ZooKeeper zk = ZooKeepers.connect("localhost:2181" , 15000);
        ZkDistributionLock lock = new ZkDistributionLockBuilder().zk(zk).build();
        try {
            lock.tryLock(ExpireMode.IGNORE , -1);
        } finally {
            lock.unlock();
            zk.close();
        }
        
        lock = new ZkDistributionLockBuilder().build();
        try {
            lock.tryLock(ExpireMode.SECONDS , 5);
        } finally {
            lock.unlock();
        }
    }
    
    public static void testLock() throws InterruptedException {
        ZkDistributionLock lock = new ZkDistributionLockBuilder().build();
        try {
            lock.lock();
            TimeUnit.MILLISECONDS.sleep(300);
        } finally {
            lock.unlock();
        }
    }
    
    public static void testDoubleLock() {
        ZkDistributionLock lock = new ZkDistributionLockBuilder().build();
        try {
            lock.lock();
            lock.lock();
        } finally {
            lock.unlock();
            lock.unlock();
        }
    }
    
    public static void main(String[] args) throws InterruptedException, IOException {
        testTryLock();
        testLock();
        testDoubleLock();
        testMixLock();
    }

    protected static void testMixLock() throws InterruptedException {
        final ZkDistributionLock lock = new ZkDistributionLockBuilder().build();
        final CountDownLatch forOne = new CountDownLatch(1);
        final CountDownLatch forTwo = new CountDownLatch(1);
        Thread one = new Thread(() -> {
            try {
                lock.lock();
                lock.lock();
                forTwo.countDown();
            } finally {
                lock.unlock();
                try {
                    forOne.await();
                } catch (InterruptedException e) {
//                    Assert.fail();
                    Assertions.fail();
                }
                lock.unlock();
            }
        });
        Thread two = new Thread(() -> {
            try {
                forTwo.await();
                boolean hasLock = lock.tryLock();
                forOne.countDown();
//                Assert.assertFalse(hasLock);
                org.assertj.core.api.Assertions.assertThat(hasLock).isFalse();
            } catch (InterruptedException e) {
//                Assert.fail();
                Assertions.fail();
            } finally {
                lock.unlock();
            }
        });
        one.start();
        two.start();
        
        while(true) {
            if(one.isAlive() || two.isAlive()) {
                TimeUnit.SECONDS.sleep(1);
                continue;
            } else break;
        }
    }
}
