package com.github.andyshaox.zk.election;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import com.github.andyshaox.zk.election.ZkMasterElection;
import com.google.common.base.Splitter;

public class ZkMasterElectionTest {
    @Test
    public void splitterStr() {
        final Splitter splitter = Splitter.on('/').omitEmptyStrings().trimResults();
        
        String str = "/my//election/";
        List<String> ls = splitter.splitToList(str);
        Assert.assertThat(ls.size() , Matchers.is(2));
        Assert.assertThat(ls.get(0) , Matchers.is("my"));
        Assert.assertThat(ls.get(1) , Matchers.is("election"));
        
        str = "///";
        ls = splitter.splitToList(str);
        Assert.assertThat(ls.size() , Matchers.is(0));
    }
    
    @Test
    public void computeStr() {
        final Splitter splitter = Splitter.on('/').omitEmptyStrings().trimResults();
        
        String str = "/my//election/";
        List<String> ls = ZkMasterElection.computePaths(splitter.splitToList(str));
        
        Assert.assertThat(ls.size() , Matchers.is(2));
        Assert.assertThat(ls.get(0) , Matchers.is("/my"));
        Assert.assertThat(ls.get(1) , Matchers.is("/my/election"));
    }
    
    @Test
    public void sortedTest() {
        List<Long> ls = Arrays.asList(15L, 23L, 4L, 6L);
        Optional<Long> lowest = ls.stream().sorted(Long::compare).findFirst();
        Assert.assertThat(lowest.isPresent(), Matchers.is(true));
        Assert.assertThat(lowest.get() , Matchers.is(4L));
    }
}
