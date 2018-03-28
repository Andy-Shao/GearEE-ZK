package com.github.andyshaox.zk.election;

import java.util.List;
import java.util.Optional;

import com.github.andyshao.election.ElectionNode;
import com.github.andyshao.election.MasterElectAlgorithm;

/**
 * 
 * Title:<br>
 * Descript:<br>
 * Copyright: Copryright(c) Mar 28, 2018<br>
 * Encoding: UNIX UTF-8
 * @author weichuang.shao
 *
 */
public class ZkMasterElectAlgorithm implements MasterElectAlgorithm {

    @Override
    public Optional<ElectionNode> findMaster(List<ElectionNode> nodes) {
        return nodes.stream()
                .filter(item -> item != null)
                .filter(item -> !(item.getName() == null || item.getName().trim().isEmpty()))
                .sorted((left, right) -> Long.compare(takeNum(left.getName()) , takeNum(right.getName())))
                .findFirst();
    }

    static final Long takeNum(String name) {
        return Long.valueOf(name.substring(name.lastIndexOf('_') + 1 , name.length()));
    }
}
