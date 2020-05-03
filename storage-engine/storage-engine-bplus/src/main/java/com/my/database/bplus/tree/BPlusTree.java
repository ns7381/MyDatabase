package com.my.database.bplus.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class BPlusTree implements Serializable {

    private Node root;
    private int degree = 4;
    private int height = 1;
    private int size = 0;

    public BPlusTree(int degree) {
        this.degree = degree;
        root = new Node(degree);
    }


    public ArrayList<String> search(Comparable key) {
        if (key == null) {
            return new ArrayList<>();
        }
        ArrayList<String> list = new ArrayList<>();
        String toRet = search(key, root, height, false);
        if (toRet != null) {
            list.add(toRet);
        }
        return list;
    }

    public String search(Comparable key, Node node, int height, boolean delete) {
        if (height == 1) {
            ArrayList<Entry> leafEntries = node.nodeData;
            for (Entry leafEntry : leafEntries) {
                if (key.compareTo(leafEntry.key) == 0) {
                    if (delete) {
                        ((LeafEntry) leafEntry).setDeleted(true);
                    }
                    if (!((LeafEntry) leafEntry).isDeleted()) {
                        return ((LeafEntry) leafEntry).getValue();
                    }
                }
            }
        } else {
            for (int i = 0; i < node.nodeData.size(); i++) {
                if (key.compareTo(node.nodeData.get(i).key) < 0) {
                    return search(key, node.nodeData.get(i).leftChild, --height, delete);
                }
            }
            // key greater go to right child of node
            return search(key, node.rightChild, --height, delete);
        }
        return null;
    }

    public void insert(Comparable key, String value) {
        if (key == null) {
            throw new NullPointerException("key is nul or value is null");
        }
        Node x = insert(key, value, root, height);
        // case 4 new Root
        if (x != null) {
            ArrayList<Entry> newRootEntry = new ArrayList<Entry>();
            newRootEntry.add(new Entry(x.nodeData.get(0).key));
            if (height != 1) {
                x.nodeData.remove(0);
            }
            Node newRoot = new Node(degree, newRootEntry);
            newRoot.rightChild = x;
            newRoot.nodeData.get(0).leftChild = root;
            this.root = newRoot;
            height++;
        }
        size++;
    }

    private Node insert(Comparable key, String value, Node node, int height) {
        // leaves
        if (height == 1) {
            LeafEntry toInsert = new LeafEntry(key, value);
            node.nodeData.add(toInsert);
            // malhash right child
            Collections.sort(node.nodeData);
            // case 1
            if (!node.isOverFlow()) {
                return null;
            } else {
                // return new splitted node, leave old one case 2
                return node.splitLeaf();
            }
        }
        /* Now to the Inner Nodes */
        Node prevSplit = null;
        boolean called = false;
        for (int i = 0; i < node.nodeData.size(); i++) {
            if (key.compareTo(node.nodeData.get(i).key) < 0) {
                called = true;
                prevSplit = insert(key, value, node.nodeData.get(i).leftChild, --height);
                break;
            }
        }
        if (!called) {
            prevSplit = insert(key, value, node.rightChild, --height);
        }

        if (prevSplit == null) {
            return null;
        }

        Entry toInsertNonLeaf = new Entry(prevSplit.nodeData.get(0).key);
        if (!(prevSplit.nodeData.get(0) instanceof LeafEntry)) {
            prevSplit.nodeData.remove(0);
        }
        toInsertNonLeaf.leftChild = node.rightChild;
        node.nodeData.add(toInsertNonLeaf);
        node.rightChild = prevSplit;
        Collections.sort(node.nodeData);
        if (!node.isOverFlow()) {
            return null;
        } else {
            return node.split();
        }
    }

    public void delete(Comparable key) {
		search(key, root, height,true);
	}
}
