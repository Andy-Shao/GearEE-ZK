package com.github.andyshaox.zk.group;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.stream.Collectors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.github.andyshao.distribution.group.GroupManageException;
import com.github.andyshao.distribution.group.GroupManagement;
import com.github.andyshao.distribution.group.MemberJoin;
import com.github.andyshao.distribution.group.MemberNode;
import com.github.andyshaox.zk.utils.ZooKeepers;

import lombok.AccessLevel;
import lombok.Setter;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) Mar 29, 2018<br>
 * Encoding: UNIX UTF-8
 * @author andy.shao
 *
 */
public class ZkGroupManagement implements GroupManagement {
    @Setter(value = AccessLevel.PACKAGE)
    private String rootPath;
    @Setter(value = AccessLevel.PACKAGE)
    private String nodeHead;
    @Setter(value = AccessLevel.PACKAGE)
    private String connectString;
    @Setter(value = AccessLevel.PACKAGE)
    private int sessionTimeOut;
    @Setter(value = AccessLevel.PACKAGE)
    private volatile ZooKeeper zk;
    private volatile String nodeName;

    @Override
    public void joinGroup(final MemberJoin join) throws GroupManageException {
        connectZk();
        createPaths();
        register(join);
        
        joining(join);
    }

    protected void joining(final MemberJoin join) {
        try {
            join.onMembersChange(zk.getChildren(rootPath , watch -> {
                EventType type = watch.getType();
                switch(type) {
                case NodeChildrenChanged:
                    joining(join);
                    break;
                default:
                    break;
                }
            })
            .stream()
            .map(child -> {
                byte[] bs = null;
                try {
                    bs = zk.getData(String.format("%s/%s" , rootPath, child) , false , null);
                } catch (KeeperException | InterruptedException e) {
                    throw new GroupManageException(e);
                }
                return readObj(bs);
            })
            .collect(Collectors.toList()));
        } catch (KeeperException | InterruptedException e) {
            throw new GroupManageException(e);
        }
    }

    protected MemberNode readObj(byte[] bs) {
        try {
            try(ByteArrayInputStream data = new ByteArrayInputStream(bs);
                    ObjectInputStream input = new ObjectInputStream(data);){
                return (MemberNode)input.readObject();
            }
        } catch (ClassNotFoundException | IOException e) {
            throw new GroupManageException(e);
        }
    }

    protected void register(MemberJoin join) {
        if(nodeName == null) try {
            nodeName = zk.create(String.format("%s/%s" , rootPath, nodeHead) , writeObj(join) , Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException | InterruptedException e) {
            throw new GroupManageException(e);
        }
    }

    protected byte[] writeObj(MemberJoin join) {
        byte[] bs = null;
        try(ByteArrayOutputStream data = new ByteArrayOutputStream();
                ObjectOutputStream output = new ObjectOutputStream(data);){
            output.writeObject(join.selfNode());
            bs = data.toByteArray();
        } catch (IOException e) {
            throw new GroupManageException(e);
        }
        return bs;
    }

    protected void createPaths() {
        try {
            ZooKeepers.createPathRecursively(zk , rootPath);
        } catch (KeeperException | InterruptedException e) {
            throw new GroupManageException(e);
        }
    }

    protected synchronized void connectZk() {
        if(zk != null) return;
        try {
            zk = ZooKeepers.connect(connectString , sessionTimeOut);
        } catch (InterruptedException | IOException e) {
            throw new GroupManageException(e);
        }
    }

    @Override
    public synchronized void cancel() throws GroupManageException {
        if(zk != null && nodeName != null) {
            try {
                Stat stat = zk.exists(fullNodePath() , false);
                if(stat != null) zk.delete(fullNodePath() , -1);
                nodeName = null;
            } catch (KeeperException | InterruptedException e) {
                throw new GroupManageException(e);
            }
        }
    }
    
    public synchronized void refresh() {
        if(zk != null) {
            try {
                zk.close();
                zk = null;
            } catch (InterruptedException e) {
                throw new GroupManageException(e);
            }
        }
    }

    protected String fullNodePath() {
        return String.format("%s/%s" , rootPath, nodeName);
    }

}
