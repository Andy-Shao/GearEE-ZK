package com.github.andyshaox.zk.election;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.andyshao.distribution.election.Election;
import com.github.andyshao.distribution.election.ElectionNode;
import com.github.andyshao.distribution.election.MasterElect;
import com.github.andyshao.lang.number.ByteLevel;
import com.github.andyshao.lang.number.SimpleByteSize;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MasterElectionIntegrationTest {
    
    public static void main(String[] args) {
        Election election = null;
        try {
            election = new ZkMasterElectBuilder().build();
            election.elect(new MasterElect() {
                
                @Override
                public ElectionNode selfNode() {
                    ElectionNode node = new ElectionNode();
                    node.setCpuNum(4);
                    node.setHost("localhost");
                    node.setPort(8080);
                    node.setMemory(new SimpleByteSize(2 , ByteLevel.GB));
                    Map<String , ? super Serializable> pros = node.getPros();
                    pros.put("my-key" , "my-value");
                    return node ;
                }
                
                @Override
                public void onMasterChange(ElectionNode master) {
                    log.info("Master is {}", master);
                }
                
                @Override
                public void onElectMembersChange(List<ElectionNode> electionNodes) {
                    List<String> collect = electionNodes.stream().map(ElectionNode::getName).collect(Collectors.toList());
                    log.info("nodes list is {}", collect);
                }
            });
        } finally {
            if(election != null) election.cancel();
        }
    }
}
