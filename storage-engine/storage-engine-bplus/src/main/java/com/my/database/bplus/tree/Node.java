package com.my.database.bplus.tree;

import java.io.Serializable;
import java.util.ArrayList;

public class Node implements Serializable {
    private int degree;
    ArrayList<Entry> nodeData;
    Node rightChild;
    Node next;

    public Node(int degree) {
        this.degree = degree;
        nodeData = new ArrayList<Entry>();
    }

    public Node(int degree, ArrayList<Entry> nodeData) {
        this.degree = degree;
        this.nodeData = nodeData;
    }

    public boolean isOverFlow() {
        return nodeData.size() >= degree;
    }

    public Node splitLeaf() {
        ArrayList<Entry> newEntry = new ArrayList<Entry>();
        int halfSize = nodeData.size() / 2;
        for (int i = halfSize; i < nodeData.size(); i++) {
            LeafEntry currEntry = (LeafEntry) nodeData.get(i);
            newEntry.add(currEntry);
        }
        for (int i = 0; i < newEntry.size(); i++) {
            nodeData.remove(newEntry.get(i));
        }
        Node newNode = new Node(degree, newEntry);
        newNode.next = next;
        next = newNode;
        return newNode;
    }

    /**
     * splits non Leaf node but then you need to remove the 1st element to make sparse and rise it up
     */
    public Node split() {
        ArrayList<Entry> newEntry = new ArrayList<Entry>();
        int halfSize = nodeData.size() / 2;
        for (int i = halfSize; i < nodeData.size(); i++) {
            Entry currEntry = nodeData.get(i);
            newEntry.add(currEntry);
        }
        for (Entry entry : newEntry) {
            nodeData.remove(entry);
        }
        Node newNode = new Node(degree, newEntry);
        newNode.rightChild = rightChild;
        rightChild = newEntry.get(0).leftChild;
        return newNode;
    }
}
