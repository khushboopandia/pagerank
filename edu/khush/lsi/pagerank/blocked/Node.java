package edu.khush.lsi.pagerank.blocked;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class Node implements Iterable<Long>, Writable {

	long blockID;
	long nodeID;
	double pageRank;
	ArrayList<Long> outgoingEdges; // node ids of outgoing edges

	public Node() {
		nodeID = -1;
		blockID = -1;
		outgoingEdges = new ArrayList<Long>();
	}

	// Construct a node with no outgoing links.
	public Node(long nid) {
		nodeID = nid;
		blockID = -1;
		outgoingEdges = new ArrayList<Long>();

	}

	// Construct a node with no outgoing links.
	public Node(long nid, ArrayList<Long> outgoingEdges) {
		this.nodeID = nid;
		this.blockID = -1;
		this.outgoingEdges = outgoingEdges;

	}

	@Override
	public void readFields(DataInput in) throws IOException {

		nodeID = in.readLong();
		blockID = in.readLong();
		pageRank = in.readDouble();

		long next = in.readLong();
		outgoingEdges = new ArrayList<Long>();
		while (next != -1) {
			outgoingEdges.add(next);
			next = in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeLong(nodeID);
		out.writeLong(blockID);
		out.writeDouble(pageRank);

		for (long n : outgoingEdges) {
			out.writeLong(n);
		}
		out.writeLong(-1);

	}

	@Override
	public Iterator<Long> iterator() {
		return outgoingEdges.iterator();
	}

	public long getBlockID() {
		return blockID;
	}

	public void setBlockID(long blockID) {
		this.blockID = blockID;
	}

	public long getNodeID() {
		return nodeID;
	}

	public void setNodeID(long nodeID) {
		this.nodeID = nodeID;
	}

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public ArrayList<Long> getOutgoingEdges() {
		return outgoingEdges;
	}

	public void setOutgoingEdges(ArrayList<Long> outgoingEdges) {
		this.outgoingEdges = outgoingEdges;
	}

}
