package com.github.andyshaox.zk.election;

import java.util.List;
import java.util.Optional;

import com.github.andyshao.election.ElectionNode;
import com.github.andyshao.election.MasterElectAlgorithm;
import com.github.andyshao.lang.StringOperation;

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
                .sorted((left, right) -> StringOperation.COMPARATOR.compare(left.getName() , right.getName()))
                .findFirst();
    }
}
