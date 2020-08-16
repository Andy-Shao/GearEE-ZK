package com.github.andyshaox.zk.election;

import com.github.andyshao.distribution.election.ElectionNode;
import com.github.andyshao.lang.number.ByteLevel;
import com.github.andyshao.lang.number.SimpleByteSize;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class ZkMasterElectAlgorithmTest {
    private List<ElectionNode> nodes;
    
    @BeforeEach
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
//        Assert.assertTrue(master.isPresent());
//        Assert.assertThat(master.get().getName() , Matchers.is("node_001"));
        Assertions.assertThat(master.isPresent()).isTrue();
        Assertions.assertThat(master.get().getName()).isEqualTo("node_001");
    }
}
