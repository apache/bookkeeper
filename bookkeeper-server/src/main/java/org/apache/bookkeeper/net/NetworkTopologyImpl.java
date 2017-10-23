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
package org.apache.bookkeeper.net;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class represents a cluster of computer with a tree hierarchical
 * network topology.
 * For example, a cluster may be consists of many data centers filled
 * with racks of computers.
 * In a network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers
 * or racks.
 *
 */
public class NetworkTopologyImpl implements NetworkTopology {

    public final static int DEFAULT_HOST_LEVEL = 2;
    public static final Logger LOG = LoggerFactory.getLogger(NetworkTopologyImpl.class);

    public static class InvalidTopologyException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public InvalidTopologyException(String msg) {
            super(msg);
        }
    }

    /** InnerNode represents a switch/router of a data center or rack.
     * Different from a leaf node, it has non-null children.
     */
    static class InnerNode extends NodeBase {
        protected List<Node> children = new ArrayList<Node>();
        private int numOfLeaves;

        /** Construct an InnerNode from a path-like string */
        InnerNode(String path) {
            super(path);
        }

        /** Construct an InnerNode from its name and its network location */
        InnerNode(String name, String location) {
            super(name, location);
        }

        /** Construct an InnerNode
         * from its name, its network location, its parent, and its level */
        InnerNode(String name, String location, InnerNode parent, int level) {
            super(name, location, parent, level);
        }

        /** @return its children */
        List<Node> getChildren() {
            return children;
        }

        /** @return the number of children this node has */
        int getNumOfChildren() {
            return children.size();
        }

        /** Judge if this node represents a rack
         * @return true if it has no child or its children are not InnerNodes
         */
        boolean isRack() {
            if (children.isEmpty()) {
                return true;
            }

            Node firstChild = children.get(0);
            if (firstChild instanceof InnerNode) {
                return false;
            }

            return true;
        }

        /** Judge if this node is an ancestor of node <i>n</i>
         *
         * @param n a node
         * @return true if this node is an ancestor of <i>n</i>
         */
        boolean isAncestor(Node n) {
            return getPath(this).equals(NodeBase.PATH_SEPARATOR_STR)
                    || (n.getNetworkLocation() + NodeBase.PATH_SEPARATOR_STR).startsWith(getPath(this)
                            + NodeBase.PATH_SEPARATOR_STR);
        }

        /** Judge if this node is the parent of node <i>n</i>
         *
         * @param n a node
         * @return true if this node is the parent of <i>n</i>
         */
        boolean isParent(Node n) {
            return n.getNetworkLocation().equals(getPath(this));
        }

        /* Return a child name of this node who is an ancestor of node <i>n</i> */
        private String getNextAncestorName(Node n) {
            if (!isAncestor(n)) {
                throw new IllegalArgumentException(this + "is not an ancestor of " + n);
            }
            String name = n.getNetworkLocation().substring(getPath(this).length());
            if (name.charAt(0) == PATH_SEPARATOR) {
                name = name.substring(1);
            }
            int index = name.indexOf(PATH_SEPARATOR);
            if (index != -1)
                name = name.substring(0, index);
            return name;
        }

        /** Add node <i>n</i> to the subtree of this node
         * @param n node to be added
         * @return true if the node is added; false otherwise
         */
        boolean add(Node n) {
            if (!isAncestor(n))
                throw new IllegalArgumentException(n.getName() + ", which is located at " + n.getNetworkLocation()
                        + ", is not a decendent of " + getPath(this));
            if (isParent(n)) {
                // this node is the parent of n; add n directly
                n.setParent(this);
                n.setLevel(this.level + 1);
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i).getName().equals(n.getName())) {
                        children.set(i, n);
                        return false;
                    }
                }
                children.add(n);
                numOfLeaves++;
                return true;
            } else {
                // find the next ancestor node
                String parentName = getNextAncestorName(n);
                InnerNode parentNode = null;
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i).getName().equals(parentName)) {
                        parentNode = (InnerNode) children.get(i);
                        break;
                    }
                }
                if (parentNode == null) {
                    // create a new InnerNode
                    parentNode = createParentNode(parentName);
                    children.add(parentNode);
                }
                // add n to the subtree of the next ancestor node
                if (parentNode.add(n)) {
                    numOfLeaves++;
                    return true;
                } else {
                    return false;
                }
            }
        }

        /**
         * Creates a parent node to be added to the list of children.
         * Creates a node using the InnerNode four argument constructor specifying
         * the name, location, parent, and level of this node.
         *
         * <p>To be overridden in subclasses for specific InnerNode implementations,
         * as alternative to overriding the full {@link #add(Node)} method.
         *
         * @param parentName The name of the parent node
         * @return A new inner node
         * @see InnerNode#InnerNode(String, String, InnerNode, int)
         */
        protected InnerNode createParentNode(String parentName) {
            return new InnerNode(parentName, getPath(this), this, this.getLevel() + 1);
        }

        /** Remove node <i>n</i> from the subtree of this node
         * @param n node to be deleted
         * @return true if the node is deleted; false otherwise
         */
        boolean remove(Node n) {
            String parent = n.getNetworkLocation();
            String currentPath = getPath(this);
            if (!isAncestor(n))
                throw new IllegalArgumentException(n.getName() + ", which is located at " + parent
                        + ", is not a descendent of " + currentPath);
            if (isParent(n)) {
                // this node is the parent of n; remove n directly
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i).getName().equals(n.getName())) {
                        children.remove(i);
                        numOfLeaves--;
                        n.setParent(null);
                        return true;
                    }
                }
                return false;
            } else {
                // find the next ancestor node: the parent node
                String parentName = getNextAncestorName(n);
                InnerNode parentNode = null;
                int i;
                for (i = 0; i < children.size(); i++) {
                    if (children.get(i).getName().equals(parentName)) {
                        parentNode = (InnerNode) children.get(i);
                        break;
                    }
                }
                if (parentNode == null) {
                    return false;
                }
                // remove n from the parent node
                boolean isRemoved = parentNode.remove(n);
                // if the parent node has no children, remove the parent node too
                if (isRemoved) {
                    if (parentNode.getNumOfChildren() == 0) {
                        children.remove(i);
                    }
                    numOfLeaves--;
                }
                return isRemoved;
            }
        } // end of remove

        /** Given a node's string representation, return a reference to the node
         * @param loc string location of the form /rack/node
         * @return null if the node is not found or the childnode is there but
         * not an instance of {@link InnerNode}
         */
        private Node getLoc(String loc) {
            if (loc == null || loc.length() == 0)
                return this;

            String[] path = loc.split(PATH_SEPARATOR_STR, 2);
            Node childnode = null;
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i).getName().equals(path[0])) {
                    childnode = children.get(i);
                }
            }
            if (childnode == null)
                return null; // non-existing node
            if (path.length == 1)
                return childnode;
            if (childnode instanceof InnerNode) {
                return ((InnerNode) childnode).getLoc(path[1]);
            } else {
                return null;
            }
        }

        /** get <i>leafIndex</i> leaf of this subtree
         * if it is not in the <i>excludedNode</i>
         *
         * @param leafIndex an indexed leaf of the node
         * @param excludedNode an excluded node (can be null)
         * @return
         */
        Node getLeaf(int leafIndex, Node excludedNode) {
            int count = 0;
            // check if the excluded node a leaf
            boolean isLeaf = excludedNode == null || !(excludedNode instanceof InnerNode);
            // calculate the total number of excluded leaf nodes
            int numOfExcludedLeaves = isLeaf ? 1 : ((InnerNode) excludedNode).getNumOfLeaves();
            if (isLeafParent()) { // children are leaves
                if (isLeaf) { // excluded node is a leaf node
                    int excludedIndex = children.indexOf(excludedNode);
                    if (excludedIndex != -1 && leafIndex >= 0) {
                        // excluded node is one of the children so adjust the leaf index
                        leafIndex = leafIndex >= excludedIndex ? leafIndex + 1 : leafIndex;
                    }
                }
                // range check
                if (leafIndex < 0 || leafIndex >= this.getNumOfChildren()) {
                    return null;
                }
                return children.get(leafIndex);
            } else {
                for (int i = 0; i < children.size(); i++) {
                    InnerNode child = (InnerNode) children.get(i);
                    if (excludedNode == null || excludedNode != child) {
                        // not the excludedNode
                        int numOfLeaves = child.getNumOfLeaves();
                        if (excludedNode != null && child.isAncestor(excludedNode)) {
                            numOfLeaves -= numOfExcludedLeaves;
                        }
                        if (count + numOfLeaves > leafIndex) {
                            // the leaf is in the child subtree
                            return child.getLeaf(leafIndex - count, excludedNode);
                        } else {
                            // go to the next child
                            count = count + numOfLeaves;
                        }
                    } else { // it is the excluededNode
                        // skip it and set the excludedNode to be null
                        excludedNode = null;
                    }
                }
                return null;
            }
        }

        protected boolean isLeafParent() {
            return isRack();
        }

        /**
          * Determine if children a leaves, default implementation calls {@link #isRack()}
          * <p>To be overridden in subclasses for specific InnerNode implementations,
          * as alternative to overriding the full {@link #getLeaf(int, Node)} method.
          *
          * @return true if children are leaves, false otherwise
          */
        protected boolean areChildrenLeaves() {
            return isRack();
        }

        /**
         * Get number of leaves.
         */
        int getNumOfLeaves() {
            return numOfLeaves;
        }
    } // end of InnerNode

    /**
     * the root cluster map
     */
    InnerNode clusterMap;
    /** Depth of all leaf nodes */
    private int depthOfAllLeaves = -1;
    /** rack counter */
    protected int numOfRacks = 0;
    /** the lock used to manage access */
    protected ReadWriteLock netlock = new ReentrantReadWriteLock();

    public NetworkTopologyImpl() {
        clusterMap = new InnerNode(InnerNode.ROOT);
    }

    /** Add a leaf node
     * Update node counter & rack counter if necessary
     * @param node node to be added; can be null
     * @exception IllegalArgumentException if add a node to a leave
                                           or node to be added is not a leaf
     */
    public void add(Node node) {
        if (node == null)
            return;
        String oldTopoStr = this.toString();
        if (node instanceof InnerNode) {
            throw new IllegalArgumentException("Not allow to add an inner node: " + NodeBase.getPath(node));
        }
        int newDepth = NodeBase.locationToDepth(node.getNetworkLocation()) + 1;
        netlock.writeLock().lock();
        try {
            if ((depthOfAllLeaves != -1) && (depthOfAllLeaves != newDepth)) {
                LOG.error("Error: can't add leaf node {} at depth {} to topology:\n{}", node, newDepth, oldTopoStr);
                throw new InvalidTopologyException("Invalid network topology. "
                        + "You cannot have a rack and a non-rack node at the same level of the network topology.");
            }
            Node rack = getNodeForNetworkLocation(node);
            if (rack != null && !(rack instanceof InnerNode)) {
                LOG.error("Unexpected data node {} at an illegal network location", node);
                throw new IllegalArgumentException("Unexpected data node " + node.toString()
                        + " at an illegal network location");
            }
            if (clusterMap.add(node)) {
                LOG.info("Adding a new node: " + NodeBase.getPath(node));
                if (rack == null) {
                    numOfRacks++;
                }
                if (!(node instanceof InnerNode)) {
                    if (depthOfAllLeaves == -1) {
                        depthOfAllLeaves = node.getLevel();
                    }
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("NetworkTopology became:\n" + this.toString());
            }
        } finally {
            netlock.writeLock().unlock();
        }
    }

    /**
     * Return a reference to the node given its string representation.
     * Default implementation delegates to {@link #getNode(String)}.
     *
     * <p>To be overridden in subclasses for specific NetworkTopology
     * implementations, as alternative to overriding the full {@link #add(Node)}
     *  method.
     *
     * @param node The string representation of this node's network location is
     * used to retrieve a Node object.
     * @return a reference to the node; null if the node is not in the tree
     *
     * @see #add(Node)
     * @see #getNode(String)
     */
    protected Node getNodeForNetworkLocation(Node node) {
        return getNode(node.getNetworkLocation());
    }

    /**
     * Given a string representation of a rack, return its children
     * @param loc a path-like string representation of a rack
     * @return a newly allocated list with all the node's children
     */
    public List<Node> getDatanodesInRack(String loc) {
        netlock.readLock().lock();
        try {
            loc = NodeBase.normalize(loc);
            if (!NodeBase.ROOT.equals(loc)) {
                loc = loc.substring(1);
            }
            InnerNode rack = (InnerNode) clusterMap.getLoc(loc);
            if (rack == null) {
                return null;
            }
            return new ArrayList<Node>(rack.getChildren());
        } finally {
            netlock.readLock().unlock();
        }
    }

    /** Remove a node
     * Update node counter and rack counter if necessary
     * @param node node to be removed; can be null
     */
    @Override
    public void remove(Node node) {
        if (node == null)
            return;
        if (node instanceof InnerNode) {
            throw new IllegalArgumentException("Not allow to remove an inner node: " + NodeBase.getPath(node));
        }
        LOG.info("Removing a node: " + NodeBase.getPath(node));
        netlock.writeLock().lock();
        try {
            if (clusterMap.remove(node)) {
                InnerNode rack = (InnerNode) getNode(node.getNetworkLocation());
                if (rack == null) {
                    numOfRacks--;
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("NetworkTopology became:\n" + this.toString());
            }
        } finally {
            netlock.writeLock().unlock();
        }
    }

    /** Check if the tree contains node <i>node</i>
     *
     * @param node a node
     * @return true if <i>node</i> is already in the tree; false otherwise
     */
    @Override
    public boolean contains(Node node) {
        if (node == null)
            return false;
        netlock.readLock().lock();
        try {
            Node parent = node.getParent();
            for (int level = node.getLevel(); parent != null && level > 0; parent = parent.getParent(), level--) {
                if (parent == clusterMap) {
                    return true;
                }
            }
        } finally {
            netlock.readLock().unlock();
        }
        return false;
    }

    /** Given a string representation of a node, return its reference
     *
     * @param loc
     *          a path-like string representation of a node
     * @return a reference to the node; null if the node is not in the tree
     */
    @Override
    public Node getNode(String loc) {
        netlock.readLock().lock();
        try {
            loc = NodeBase.normalize(loc);
            if (!NodeBase.ROOT.equals(loc))
                loc = loc.substring(1);
            return clusterMap.getLoc(loc);
        } finally {
            netlock.readLock().unlock();
        }
    }

    /** Given a string representation of a rack for a specific network
     *  location
     *
     * To be overridden in subclasses for specific NetworkTopology
     * implementations, as alternative to overriding the full
     * {@link #getRack(String)} method.
     * @param loc
     *          a path-like string representation of a network location
     * @return a rack string
     */
    public String getRack(String loc) {
        return loc;
    }

    /** @return the total number of racks */
    @Override
    public int getNumOfRacks() {
        netlock.readLock().lock();
        try {
            return numOfRacks;
        } finally {
            netlock.readLock().unlock();
        }
    }

    /** @return the total number of leaf nodes */
    public int getNumOfLeaves() {
        netlock.readLock().lock();
        try {
            return clusterMap.getNumOfLeaves();
        } finally {
            netlock.readLock().unlock();
        }
    }

    /** Return the distance between two nodes
     * It is assumed that the distance from one node to its parent is 1
     * The distance between two nodes is calculated by summing up their distances
     * to their closest common ancestor.
     * @param node1 one node
     * @param node2 another node
     * @return the distance between node1 and node2 which is zero if they are the same
     *  or {@link Integer#MAX_VALUE} if node1 or node2 do not belong to the cluster
     */
    public int getDistance(Node node1, Node node2) {
        if (node1 == node2) {
            return 0;
        }
        Node n1 = node1, n2 = node2;
        int dis = 0;
        netlock.readLock().lock();
        try {
            int level1 = node1.getLevel(), level2 = node2.getLevel();
            while (n1 != null && level1 > level2) {
                n1 = n1.getParent();
                level1--;
                dis++;
            }
            while (n2 != null && level2 > level1) {
                n2 = n2.getParent();
                level2--;
                dis++;
            }
            while (n1 != null && n2 != null && n1.getParent() != n2.getParent()) {
                n1 = n1.getParent();
                n2 = n2.getParent();
                dis += 2;
            }
        } finally {
            netlock.readLock().unlock();
        }
        if (n1 == null) {
            LOG.warn("The cluster does not contain node: {}", NodeBase.getPath(node1));
            return Integer.MAX_VALUE;
        }
        if (n2 == null) {
            LOG.warn("The cluster does not contain node: {}", NodeBase.getPath(node2));
            return Integer.MAX_VALUE;
        }
        return dis + 2;
    }

    /** Check if two nodes are on the same rack
     * @param node1 one node (can be null)
     * @param node2 another node (can be null)
     * @return true if node1 and node2 are on the same rack; false otherwise
     * @exception IllegalArgumentException when either node1 or node2 is null, or
     * node1 or node2 do not belong to the cluster
     */
    public boolean isOnSameRack(Node node1, Node node2) {
        if (node1 == null || node2 == null) {
            return false;
        }

        netlock.readLock().lock();
        try {
            return isSameParents(node1, node2);
        } finally {
            netlock.readLock().unlock();
        }
    }

    /**
     * Check if network topology is aware of NodeGroup
     */
    public boolean isNodeGroupAware() {
        return false;
    }

    /**
     * Return false directly as not aware of NodeGroup, to be override in sub-class
     */
    public boolean isOnSameNodeGroup(Node node1, Node node2) {
        return false;
    }

    /**
     * Compare the parents of each node for equality
     *
     * <p>To be overridden in subclasses for specific NetworkTopology
     * implementations, as alternative to overriding the full
     * {@link #isOnSameRack(Node, Node)} method.
     *
     * @param node1 the first node to compare
     * @param node2 the second node to compare
     * @return true if their parents are equal, false otherwise
     *
     * @see #isOnSameRack(Node, Node)
     */
    protected boolean isSameParents(Node node1, Node node2) {
        return node1.getParent() == node2.getParent();
    }

    final protected static Random r = new Random();

    /** randomly choose one node from <i>scope</i>
     * if scope starts with ~, choose one from the all nodes except for the
     * ones in <i>scope</i>; otherwise, choose one from <i>scope</i>
     * @param scope range of nodes from which a node will be chosen
     * @return the chosen node
     */
    public Node chooseRandom(String scope) {
        netlock.readLock().lock();
        try {
            if (scope.startsWith("~")) {
                return chooseRandom(NodeBase.ROOT, scope.substring(1));
            } else {
                return chooseRandom(scope, null);
            }
        } finally {
            netlock.readLock().unlock();
        }
    }

    private Node chooseRandom(String scope, String excludedScope) {
        if (excludedScope != null) {
            if (scope.startsWith(excludedScope)) {
                return null;
            }
            if (!excludedScope.startsWith(scope)) {
                excludedScope = null;
            }
        }
        Node node = getNode(scope);
        if (!(node instanceof InnerNode)) {
            return node;
        }
        InnerNode innerNode = (InnerNode) node;
        int numOfDatanodes = innerNode.getNumOfLeaves();
        if (excludedScope == null) {
            node = null;
        } else {
            node = getNode(excludedScope);
            if (!(node instanceof InnerNode)) {
                numOfDatanodes -= 1;
            } else {
                numOfDatanodes -= ((InnerNode) node).getNumOfLeaves();
            }
        }
        int leaveIndex = r.nextInt(numOfDatanodes);
        return innerNode.getLeaf(leaveIndex, node);
    }

    /** return leaves in <i>scope</i>
     * @param scope a path string
     * @return leaves nodes under specific scope
     */
    private Set<Node> doGetLeaves(String scope) {
        Node node = getNode(scope);
        Set<Node> leafNodes = new HashSet<Node>();
        if (!(node instanceof InnerNode)) {
            leafNodes.add(node);
        } else {
            InnerNode innerNode = (InnerNode) node;
            for (int i = 0; i < innerNode.getNumOfLeaves(); i++) {
                leafNodes.add(innerNode.getLeaf(i, null));
            }
        }
        return leafNodes;
    }

    @Override
    public Set<Node> getLeaves(String scope) {
        netlock.readLock().lock();
        try {
            if (scope.startsWith("~")) {
                Set<Node> allNodes = doGetLeaves(NodeBase.ROOT);
                Set<Node> excludeNodes = doGetLeaves(scope.substring(1));
                allNodes.removeAll(excludeNodes);
                return allNodes;
            } else {
                return doGetLeaves(scope);
            }
        } finally {
            netlock.readLock().unlock();
        }
    }

    /** return the number of leaves in <i>scope</i> but not in <i>excludedNodes</i>
     * if scope starts with ~, return the number of nodes that are not
     * in <i>scope</i> and <i>excludedNodes</i>;
     * @param scope a path string that may start with ~
     * @param excludedNodes a list of nodes
     * @return number of available nodes
     */
    public int countNumOfAvailableNodes(String scope, Collection<Node> excludedNodes) {
        boolean isExcluded = false;
        if (scope.startsWith("~")) {
            isExcluded = true;
            scope = scope.substring(1);
        }
        scope = NodeBase.normalize(scope);
        int count = 0; // the number of nodes in both scope & excludedNodes
        netlock.readLock().lock();
        try {
            for (Node node : excludedNodes) {
                if ((NodeBase.getPath(node) + NodeBase.PATH_SEPARATOR_STR).startsWith(scope
                        + NodeBase.PATH_SEPARATOR_STR)) {
                    count++;
                }
            }
            Node n = getNode(scope);
            int scopeNodeCount = 1;
            if (n instanceof InnerNode) {
                scopeNodeCount = ((InnerNode) n).getNumOfLeaves();
            }
            if (isExcluded) {
                return clusterMap.getNumOfLeaves() - scopeNodeCount - excludedNodes.size() + count;
            } else {
                return scopeNodeCount - count;
            }
        } finally {
            netlock.readLock().unlock();
        }
    }

    /** convert a network tree to a string */
    @Override
    public String toString() {
        // print the number of racks
        StringBuilder tree = new StringBuilder();
        tree.append("Number of racks: ");
        tree.append(numOfRacks);
        tree.append("\n");
        // print the number of leaves
        int numOfLeaves = getNumOfLeaves();
        tree.append("Expected number of leaves:");
        tree.append(numOfLeaves);
        tree.append("\n");
        // print nodes
        for (int i = 0; i < numOfLeaves; i++) {
            tree.append(NodeBase.getPath(clusterMap.getLeaf(i, null)));
            tree.append("\n");
        }
        return tree.toString();
    }

    /**
     * Divide networklocation string into two parts by last separator, and get
     * the first part here.
     *
     * @param networkLocation
     * @return
     */
    public static String getFirstHalf(String networkLocation) {
        int index = networkLocation.lastIndexOf(NodeBase.PATH_SEPARATOR_STR);
        return networkLocation.substring(0, index);
    }

    /**
     * Divide networklocation string into two parts by last separator, and get
     * the second part here.
     *
     * @param networkLocation
     * @return
     */
    public static String getLastHalf(String networkLocation) {
        int index = networkLocation.lastIndexOf(NodeBase.PATH_SEPARATOR_STR);
        return networkLocation.substring(index);
    }

    /** swap two array items */
    static protected void swap(Node[] nodes, int i, int j) {
        Node tempNode;
        tempNode = nodes[j];
        nodes[j] = nodes[i];
        nodes[i] = tempNode;
    }

    /** Sort nodes array by their distances to <i>reader</i>
     * It linearly scans the array, if a local node is found, swap it with
     * the first element of the array.
     * If a local rack node is found, swap it with the first element following
     * the local node.
     * If neither local node or local rack node is found, put a random replica
     * location at position 0.
     * It leaves the rest nodes untouched.
     * @param reader the node that wishes to read a block from one of the nodes
     * @param nodes the list of nodes containing data for the reader
     */
    public void pseudoSortByDistance(Node reader, Node[] nodes) {
        int tempIndex = 0;
        int localRackNode = -1;
        if (reader != null) {
            //scan the array to find the local node & local rack node
            for (int i = 0; i < nodes.length; i++) {
                if (tempIndex == 0 && reader == nodes[i]) { //local node
                    //swap the local node and the node at position 0
                    if (i != 0) {
                        swap(nodes, tempIndex, i);
                    }
                    tempIndex = 1;
                    if (localRackNode != -1) {
                        if (localRackNode == 0) {
                            localRackNode = i;
                        }
                        break;
                    }
                } else if (localRackNode == -1 && isOnSameRack(reader, nodes[i])) {
                    //local rack
                    localRackNode = i;
                    if (tempIndex != 0)
                        break;
                }
            }

            // swap the local rack node and the node at position tempIndex
            if (localRackNode != -1 && localRackNode != tempIndex) {
                swap(nodes, tempIndex, localRackNode);
                tempIndex++;
            }
        }

        // put a random node at position 0 if it is not a local/local-rack node
        if (tempIndex == 0 && localRackNode == -1 && nodes.length != 0) {
            swap(nodes, 0, r.nextInt(nodes.length));
        }
    }

}
