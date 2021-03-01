package org.apache.bookkeeper.net;

import static org.junit.Assert.assertTrue;

import java.util.Set;
import org.junit.Test;

public class NetworkTopologyImplTest {

  @Test
  public void getLeavesShouldReturnEmptySetForNonExistingScope() {
    NetworkTopologyImpl networkTopology = new NetworkTopologyImpl();
    final Set<Node> leaves = networkTopology.getLeaves("/non-existing-scope");
    assertTrue(leaves.isEmpty());
  }
}