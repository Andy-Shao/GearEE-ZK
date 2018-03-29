package com.github.andyshaox.zk.group;

import org.apache.zookeeper.ZooKeeper;

import com.github.andyshao.util.EntityOperation;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) Mar 29, 2018<br>
 * Encoding: UNIX UTF-8
 * @author andy.shao
 *
 */
@SuppressWarnings("unused")
public class ZkGroupManagementBuilder {
    private String rootPath = "/membership";
    private String nodeHead = "member-";
    private String connectString = "localhost:2181";
    private int sessionTimeOut = 15000;
    private ZooKeeper zk;
    
    public ZkGroupManagementBuilder zk(ZooKeeper zk) {
        ZkGroupManagementBuilder ret = copy();
        ret.zk = zk;
        return ret ;
    }
    
    public ZkGroupManagementBuilder sessionTimeOut(int sessionTimeOut) {
        ZkGroupManagementBuilder ret = copy();
        ret.sessionTimeOut = sessionTimeOut;
        return ret;
    }
    
    public ZkGroupManagementBuilder connectString(String connectString) {
        ZkGroupManagementBuilder ret = copy();
        ret.connectString = connectString;
        return ret ;
    }
    
    public ZkGroupManagementBuilder nodeHead(String nodeHead) {
        ZkGroupManagementBuilder ret = copy();
        ret.nodeHead = nodeHead;
        return ret;
    }
    
    public ZkGroupManagementBuilder rootPath(String rootPath) {
        ZkGroupManagementBuilder ret = copy();
        ret.rootPath = rootPath;
        return ret;
    }
    
    public ZkGroupManagementBuilder copy() {
        ZkGroupManagementBuilder copy = new ZkGroupManagementBuilder();
        EntityOperation.copyProperties(this , copy);
        return copy;
    }
    
    public ZkGroupManagement build() {
        ZkGroupManagement ret = new ZkGroupManagement();
        EntityOperation.copyProperties(this , ret);
        return ret;
    }
}
