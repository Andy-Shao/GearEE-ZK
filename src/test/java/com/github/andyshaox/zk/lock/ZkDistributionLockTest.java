package com.github.andyshaox.zk.lock;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import rx.Observable;
import rx.Observer;

public class ZkDistributionLockTest {
    @Test
    public void testSort() {
        List<String> ls = Arrays.asList("lock-003", "lock-002", "lock-007", "lock-001", "lock-009");
        Optional<String> lowest = ls.stream().sorted().findFirst();
        Assert.assertTrue(lowest.isPresent());
        Assert.assertThat(lowest.get() , Matchers.is("lock-001"));
    }
    
    @Test
    public void testTimeTask() throws InterruptedException {
        final AtomicBoolean hasDoneIt = new AtomicBoolean(false);
        Observable.<Void>unsafeCreate(v -> {
            try {
                TimeUnit.MICROSECONDS.sleep(100);
                v.onCompleted();
            } catch (InterruptedException e1) {
                v.onError(e1);
            }
        }).subscribe(new Observer<Void>() {

            @Override
            public void onCompleted() {
                hasDoneIt.set(true);
            }

            @Override
            public void onError(Throwable e) {
                Assert.fail();
            }

            @Override
            public void onNext(Void t) {
                Assert.fail();
            }
            
        });
        TimeUnit.MICROSECONDS.sleep(1000);
        Assert.assertTrue(hasDoneIt.get());
    }
    
    @Test
    public void testCountDown() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Assert.assertThat(countDownLatch.getCount() , Matchers.is(1L));
        countDownLatch.countDown();
        Assert.assertThat(countDownLatch.getCount() , Matchers.is(0L));
        countDownLatch.await();
    }
}
