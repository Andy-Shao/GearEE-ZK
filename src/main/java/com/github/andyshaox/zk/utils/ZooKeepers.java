package com.github.andyshaox.zk.utils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.github.andyshaox.zk.election.ZkMasterElection;
import com.google.common.base.Splitter;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) Mar 28, 2018<br>
 * Encoding: UNIX UTF-8
 * @author andy.shao
 *
 */
public final class ZooKeepers {
    private ZooKeepers() {}

    public static final ZooKeeper connect(String connectString , int sessionTimeOut) throws InterruptedException, IOException{
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            return new ZooKeeper(connectString , sessionTimeOut , (event)->{
                final KeeperState keeperState = event.getState();
                switch(keeperState) {
                case SyncConnected:
                    countDownLatch.countDown();
                    break;
                default:
                    break;
                }
            });
        } finally {
            countDownLatch.await();
        }
    }
    
    public static final void createPathRecursively(ZooKeeper zk , String rootPath) throws KeeperException, InterruptedException {
        List<String> pathes = ZkMasterElection.computePaths(Splitter.on('/').omitEmptyStrings().trimResults().splitToList(rootPath));
        for(String path : pathes) {
            Stat stat;
            try {
                stat = zk.exists(path , false);
                if(stat == null) zk.create(path , "".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.PERSISTENT);
            } catch(NodeExistsException e) {
                // do nothing - because of you need ignore the path is already exists
            }
        }
    }
}
