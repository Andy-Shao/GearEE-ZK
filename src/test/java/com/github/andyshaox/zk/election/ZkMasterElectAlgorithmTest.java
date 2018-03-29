package com.github.andyshaox.zk.election;

import java.util.List;
import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.andyshao.distribution.election.ElectionNode;
import com.github.andyshao.lang.number.ByteLevel;
import com.github.andyshao.lang.number.SimpleByteSize;
import com.google.common.collect.Lists;

public class ZkMasterElectAlgorithmTest {
    private List<ElectionNode> nodes;
    
    @Before
    public void before() {
        nodes = Lists.newArrayList();
        ElectionNode e = new ElectionNode();
        e.setCpuNum(2);
        e.setHost("127.0.0.1");
        e.setMemory(new SimpleByteSize(2 , ByteLevel.GB));
        e.setPort(8080);
        e.setName("node_001");
        nodes.add(e);
        e = new ElectionNode();
        e.setCpuNum(3);
        e.setHost("192.168.1.104");
        e.setMemory(new SimpleByteSize(1, ByteLevel.TB));
        e.setPort(80);
        e.setName("node_002");
        nodes.add(e);
    }
    
    @Test
    public void testElect() {
        ZkMasterElectAlgorithm algorithm = new ZkMasterElectAlgorithm();
        Optional<ElectionNode> master = algorithm.findMaster(nodes);
        Assert.assertTrue(master.isPresent());
        Assert.assertThat(master.get().getName() , Matchers.is("node_001"));
    }
}
