/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test balancer with exclude path provided
 */
public class TestBalancerWithExcludePath {
  private static final String firstFileName = new String("/first.txt");
  private static final Path firstFile = new Path(firstFileName);
  private static final String secondFileName = new String("/second.txt");
  private static final Path secondFile = new Path(secondFileName);

  // start minicluster with two nodes and populate it with two files. Then add extra (blank) node and
  // run balancer with exclude for one file. We expect that this file's blocks remain untouched.
  @Test(timeout = 60000)
  public void testBalancerWithExcludePath () throws Exception {
    Configuration conf = new HdfsConfiguration();
    TestBalancer.initConf(conf);

    long[] capacities = new long[]{TestBalancer.CAPACITY, TestBalancer.CAPACITY};
    String[] racks = new String[]{TestBalancer.RACK0, TestBalancer.RACK1};

    int numOfDatanodes = capacities.length;
    long totalCapacity = TestBalancer.CAPACITY * numOfDatanodes;

    // each file will occupy 30% of space
    long totalUsedSpace = totalCapacity*3/10;

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(numOfDatanodes)
            .racks(racks)
            .simulatedCapacities(capacities)
            .build();
    try {
      cluster.waitActive();
      ClientProtocol client = NameNodeProxies.createProxy(conf,
              cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();
      TestBalancer.createFile(cluster, firstFile, totalUsedSpace / numOfDatanodes,
              (short)numOfDatanodes, 0);

      TestBalancer.createFile(cluster, secondFile, totalUsedSpace / numOfDatanodes,
              (short)numOfDatanodes, 0);

      // start blank datanode
      cluster.startDataNodes(conf, 1, true, null,
              new String[]{TestBalancer.RACK0}, new long[]{TestBalancer.CAPACITY});
      totalCapacity += TestBalancer.CAPACITY;

      // collect firstFile placement
      LocatedBlocks blocksBeforeBalancing = client.getBlockLocations(firstFileName, 0, totalUsedSpace);

      // run balancer
      TestBalancer.waitForHeartBeat(totalUsedSpace*2, totalCapacity, client, cluster);
      Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
      Balancer.Parameters params = new Balancer.Parameters(
              Balancer.Parameters.DEFALUT.policy,
              Balancer.Parameters.DEFALUT.threshold,
              firstFileName);
      final int r = Balancer.run(namenodes, params, conf);
      assertEquals(Balancer.ReturnStatus.NO_MOVE_PROGRESS.code, r);
      TestBalancer.waitForHeartBeat(totalUsedSpace * 2, totalCapacity, client, cluster);

      // collect firstFile placement again
      LocatedBlocks blocksAfterBalancing = client.getBlockLocations(firstFileName, 0, totalUsedSpace);

      // compare both location lists
      for (int i = 0; i < blocksBeforeBalancing.locatedBlockCount(); i++) {
        DatanodeInfo[] before = blocksBeforeBalancing.get(i).getLocations();
        DatanodeInfo[] after = blocksAfterBalancing.get(i).getLocations();

        Arrays.sort(before);
        Arrays.sort(after);
        assertArrayEquals(before, after);
      }
    } finally {
      cluster.shutdown();
    }
  }
}
