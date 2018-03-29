package com.github.andyshaox.zk.election;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Optional;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import com.github.andyshao.distribution.election.Election;
import com.github.andyshao.distribution.election.ElectionException;
import com.github.andyshao.distribution.election.ElectionNode;
import com.github.andyshao.distribution.election.MasterElect;
import com.github.andyshao.distribution.election.MasterElectAlgorithm;
import com.github.andyshaox.zk.utils.ZooKeepers;
import com.google.common.collect.Lists;

import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) Mar 27, 2018<br>
 * Encoding: UNIX UTF-8
 * @author andy.shao
 *
 */
@Slf4j
@Setter(AccessLevel.PACKAGE)
public class ZkMasterElection implements Election{
    private String leaderElectPath;
    private int sessionTimeOut;
    private String connectString;
    private String electNodeName;
    private MasterElectAlgorithm masterElectAlgorithm;
    private volatile ZooKeeper zk;
    
    @Override
    public void elect(MasterElect elect) {
        connectZk();
        createAllPath();
        registe(elect);
        
        refreshElectNodes(elect);
        electMaster(elect);
    }

    protected void electMaster(final MasterElect elect) {
        Optional<ElectionNode> leader = Optional.empty();
        do {
            if(zk == null) break;
            List<String> children = null;
            try {
                children = zk.getChildren(leaderElectPath , false);
            } catch (KeeperException | InterruptedException e) {
                throw new ElectionException(e);
            }
            leader = masterElectAlgorithm.findMaster(findAllNodes(children));
            if(!leader.isPresent()) continue;
            
            Stat leaderStat = null;
            try {
                leaderStat = zk.exists(fullPath(leader.get().getName()) , event -> {
                    switch (event.getType()) {
                    case NodeDeleted:
                        electMaster(elect);
                        break;

                    default:
                        break;
                    }
                });
            } catch (KeeperException | InterruptedException e) {
                throw new ElectionException(e);
            }
            
            if(leaderStat == null) continue;
            else break;
        } while(true);
        
        elect.onMasterChange(leader.get());
    }

    protected void refreshElectNodes(final MasterElect elect) {
        if(zk == null) return;
        List<String> children = null;
        try {
            children = zk.getChildren(leaderElectPath , (event)->{
                EventType type = event.getType();
                switch (type) {
                case NodeChildrenChanged:
                    refreshElectNodes(elect);
                    break;
                case NodeCreated:
                case NodeDataChanged:
                case NodeDeleted:
                default:
                    break;
                }});
        } catch (KeeperException | InterruptedException e) {
            throw new ElectionException(e);
        }
        List<ElectionNode> nodes = findAllNodes(children);
        elect.onElectMembersChange(nodes);
    }

    protected List<ElectionNode> findAllNodes(List<String> children) {
        List<ElectionNode> nodes = Lists.newArrayList();
        for(String child: children) {
            byte[] nodeData = null;
            try {
                nodeData = zk.getData(fullPath(child) , false , null);
            } catch (KeeperException | InterruptedException e) {
                log.warn("Try get child node has an error" , e);
                continue;
            }
            ElectionNode obj = readObject(nodeData);
            obj.setName(child);
            nodes.add(obj);
        }
        return nodes;
    }

    protected ElectionNode readObject(byte[] nodeData) {
        ElectionNode node = null;
        try(ByteArrayInputStream array = new ByteArrayInputStream(nodeData);
                ObjectInputStream input = new ObjectInputStream(array);){
            node = (ElectionNode) input.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new ElectionException(e);
        }
        return node;
    }

    protected String fullPath(String child) {
        return String.format("%s/%s" , leaderElectPath, child);
    }

    protected void registe(MasterElect elect) {
        if(elect.selfNode() == null) throw new ElectionException("selfNode cannot be null");
        try {
            byte[] bs = null;
            try(ByteArrayOutputStream array = new ByteArrayOutputStream();
                    ObjectOutputStream out = new ObjectOutputStream(array);) {
                out.writeObject(elect.selfNode());
                out.flush();
                bs = array.toByteArray();
            } catch (IOException e) {
                throw new ElectionException(e);
            }
            String nodeName = zk.create(String.format("%s/%s" , leaderElectPath, electNodeName) , bs , ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL_SEQUENTIAL);
            elect.selfNode().setName(nodeName);
        } catch (KeeperException | InterruptedException e) {
            throw new ElectionException(e);
        }
    }

    protected void createAllPath() {
        try {
            ZooKeepers.createPathRecursively(this.zk , this.leaderElectPath);
        } catch (KeeperException | InterruptedException e) {
            throw new ElectionException(e);
        }
    }

    public static List<String> computePaths(List<String> input) {
        List<String> result = Lists.newArrayList();
        for(int i=0; i<input.size(); i++) {
            result.add(computePath(input , i));
        }
        return result;
    }
    
    protected static String computePath(List<String> input, int index) {
        if(index < 0) throw new IllegalArgumentException();
        if(index == 0) return "/" + input.get(0);
        return computePath(input , index - 1) + "/" + input.get(index); 
    }

    protected synchronized ZooKeeper connectZk() {
        if(zk != null) return zk;
        try {
            return zk = ZooKeepers.connect(connectString , sessionTimeOut);
        } catch (InterruptedException | IOException e) {
            throw new ElectionException(e);
        }
    }

    @Override
    public synchronized void cancel() throws ElectionException {
        if(zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                throw new ElectionException(e);
            }
            zk = null;
        }
    }
}
