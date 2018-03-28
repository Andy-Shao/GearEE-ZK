package com.github.andyshaox.zk.lock;

import com.github.andyshao.lock.DistributionLock;
import com.github.andyshao.lock.ExpireMode;

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

    @Override
    public void unlock() {
        // TODO Auto-generated method stub

    }

    @Override
    public void lock() {
        // TODO Auto-generated method stub

    }

    @Override
    public void lock(ExpireMode mode , int times) {
        // TODO Auto-generated method stub

    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void lockInterruptibly(ExpireMode mode , int times) throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean tryLock() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean tryLock(ExpireMode mode , int times) {
        // TODO Auto-generated method stub
        return false;
    }

}
