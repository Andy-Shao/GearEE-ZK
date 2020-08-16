package com.github.andyshaox.zk.election;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.google.common.base.Splitter;

public class ZkMasterElectionTest {
    @Test
    public void splitterStr() {
        final Splitter splitter = Splitter.on('/').omitEmptyStrings().trimResults();
        
        String str = "/my//election/";
        List<String> ls = splitter.splitToList(str);
//        Assert.assertThat(ls.size() , Matchers.is(2));
//        Assert.assertThat(ls.get(0) , Matchers.is("my"));
//        Assert.assertThat(ls.get(1) , Matchers.is("election"));
        Assertions.assertThat(ls.size()).isEqualTo(2);
        Assertions.assertThat(ls.get(0)).isEqualTo("my");
        Assertions.assertThat(ls.get(1)).isEqualTo("election");
        
        str = "///";
        ls = splitter.splitToList(str);
//        Assert.assertThat(ls.size() , Matchers.is(0));
        Assertions.assertThat(ls.size()).isEqualTo(0);
    }
    
    @Test
    public void computeStr() {
        final Splitter splitter = Splitter.on('/').omitEmptyStrings().trimResults();
        
        String str = "/my//election/";
        List<String> ls = ZkMasterElection.computePaths(splitter.splitToList(str));
        
//        Assert.assertThat(ls.size() , Matchers.is(2));
//        Assert.assertThat(ls.get(0) , Matchers.is("/my"));
//        Assert.assertThat(ls.get(1) , Matchers.is("/my/election"));
        Assertions.assertThat(ls.size()).isEqualTo(2);
        Assertions.assertThat(ls.get(0)).isEqualTo("/my");
        Assertions.assertThat(ls.get(1)).isEqualTo("/my/election");
    }
    
    @Test
    public void sortedTest() {
        List<Long> ls = Arrays.asList(15L, 23L, 4L, 6L);
        Optional<Long> lowest = ls.stream().sorted(Long::compare).findFirst();
//        Assert.assertThat(lowest.isPresent(), Matchers.is(true));
//        Assert.assertThat(lowest.get() , Matchers.is(4L));
        Assertions.assertThat(lowest.isPresent()).isTrue();
        Assertions.assertThat(lowest.get()).isEqualTo(4L);
    }
}
