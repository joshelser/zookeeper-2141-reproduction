package com.github.joshelser.zookeeper;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

public class ZooKeeperAclSpam implements Runnable {
  private static final byte[] NO_DATA = new byte[0];
  private static final byte[] SMALL_DATA = new byte[256];

  private final ZooKeeper zk;

  public ZooKeeperAclSpam(ZooKeeper zk) {
    this.zk = zk;
  }

  public void run() {
    try {
      String parentZNode = "/elserj-acl-test";
      createParentZNode(parentZNode);
  
      final Set<String> createdParentZNodes = new HashSet<>();
      for (int iteration = 0; iteration < 500; iteration++) {
        System.out.println("Iteration " + iteration);
        String iterationParent = getEntryParent(parentZNode, iteration);
        for (int entries = 0; entries < 1024 * 2; entries++) {
          // Avoid putting all znodes in a single znode (would crash zk)
          if (!createdParentZNodes.contains(iterationParent)) {
            try {
              zk.create(iterationParent, NO_DATA, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
              // Make sure there are no children (that have ACLs)
              List<String> children = zk.getChildren(iterationParent, false);
              for (String child : children) {
                zk.delete(iterationParent + "/" + child, -1);
              }
            }
            createdParentZNodes.add(iterationParent);
          }
  
          // Create a new node with a new ACL
          createEntry(iterationParent, iteration, entries);
        }
        // Delete the iteration which should free up the memory
        List<String> nodesToDelete = zk.getChildren(iterationParent, false);
        for (String toDelete : nodesToDelete) {
          zk.delete(iterationParent + "/" + toDelete, -1);
        }
        zk.delete(iterationParent, -1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void createParentZNode(String parent) throws Exception {
    try {
      zk.create(parent, NO_DATA, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException e) {
      // pass
    }
  }

  private String getEntryParent(String parent, int iteration) {
    StringBuilder sb = new StringBuilder(parent);
    sb.append("/iteration_").append(iteration);
    return sb.toString();
  }

  private void createEntry(String parent, int iteration, int entries) throws Exception {
    String unique_name = iteration + "_" + entries;
    Id id = new Id("digest", unique_name + ":" + unique_name);
    ACL acl = new ACL(Perms.ALL, id);
    ACL allAcl = new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE);
    zk.create(parent + "/" + unique_name, SMALL_DATA, Arrays.asList(acl, allAcl), CreateMode.PERSISTENT);
  }

  public static void main(String[] args) throws Exception {
    ZooKeeperAclSpam patchedAclSpam = new ZooKeeperAclSpam(new ZooKeeper("localhost:2182", 60000, null));
    ZooKeeperAclSpam unpatchedAclSpam = new ZooKeeperAclSpam(new ZooKeeper("localhost:2183", 60000, null));
    //unpatchedAclSpam.run();
    patchedAclSpam.run();
    patchedAclSpam.zk.close();
    unpatchedAclSpam.zk.close();
  }
}
