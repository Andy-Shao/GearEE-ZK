package com.github.andyshaox.zk.election;

import org.apache.zookeeper.ZooKeeper;

import com.github.andyshao.election.Election;
import com.github.andyshao.election.MasterElectAlgorithm;
import com.github.andyshao.util.EntityOperation;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) Mar 27, 2018<br>
 * Encoding: UNIX UTF-8
 * @author andy.shao
 *
 */
public class ZkMasterElectBuilder{
    public static final String defaultLeaderElectPath = "/master_election";
    private String leaderElectPath = defaultLeaderElectPath;
    private int sessionTimeOut = 15000;
    private String connectString = "localhost:2181";
    private String electNodeName = "_candidate_";
    private MasterElectAlgorithm masterElectAlgorithm = new ZkMasterElectAlgorithm();
    private ZooKeeper zk;
    
    ZkMasterElectBuilder copy() {
        ZkMasterElectBuilder builder = new ZkMasterElectBuilder();
        EntityOperation.copyProperties(this , builder);
        return builder;
    }
    
    public ZkMasterElectBuilder electNodeName(String electNodeName) {
        ZkMasterElectBuilder result = this.copy();
        result.electNodeName = electNodeName;
        return result;
    }
    
    public ZkMasterElectBuilder leaderElectPath(String leaderElectPath) {
        ZkMasterElectBuilder result = this.copy();
        result.leaderElectPath = leaderElectPath;
        return result;
    }
    
    public ZkMasterElectBuilder sessionTimeOut(int sessionTimeOut) {
        ZkMasterElectBuilder result = this.copy();
        result.sessionTimeOut = sessionTimeOut;
        return result;
    }
    
    public ZkMasterElectBuilder connectString(String connectString) {
        ZkMasterElectBuilder result = this.copy();
        result.connectString = connectString;
        return result;
    }
    
    public ZkMasterElectBuilder masterElectAlgorithm(MasterElectAlgorithm algorithm) {
        ZkMasterElectBuilder result = this.copy();
        result.masterElectAlgorithm = algorithm;
        return result;
    }
    
    public ZkMasterElectBuilder zk(ZooKeeper zk) {
        ZkMasterElectBuilder result = this.copy();
        result.zk = zk;
        return result;
    }
    
    public Election build() {
        ZkMasterElection result = new ZkMasterElection();
        result.setLeaderElectPath(leaderElectPath);
        result.setConnectString(connectString);
        result.setSessionTimeOut(sessionTimeOut);
        result.setElectNodeName(electNodeName);
        result.setMasterElectAlgorithm(masterElectAlgorithm);
        result.setZk(zk);
        return result;
    }
}
