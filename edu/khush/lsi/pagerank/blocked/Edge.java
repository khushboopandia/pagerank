package edu.khush.lsi.pagerank.blocked;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Edge implements  Writable{
	
	
	long sourceNode;
	long destNode;
	double incomingPageRank;
	boolean isSameBlock;
	
	public Edge() {
		sourceNode = -1;
		destNode = -1;
	}

	

	// Construct a node with no outgoing links.
	public Edge(long source, long dest, double incomingPr,boolean isSameBlock) {
		
		this.sourceNode=source;
		this.destNode=dest;
		this.incomingPageRank=incomingPr;
		this.isSameBlock=isSameBlock;

	}

	@Override
	public void readFields(DataInput in) throws IOException {

		sourceNode = in.readLong();
		destNode = in.readLong();
		incomingPageRank = in.readDouble();
		isSameBlock=in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(sourceNode);
		out.writeLong(destNode);
		out.writeDouble(incomingPageRank);
		out.writeBoolean(isSameBlock);
		
	}



	public long getSourceNode() {
		return sourceNode;
	}



	public void setSourceNode(long sourceNode) {
		this.sourceNode = sourceNode;
	}



	public long getDestNode() {
		return destNode;
	}



	public void setDestNode(long destNode) {
		this.destNode = destNode;
	}



	public double getIncomingPageRank() {
		return incomingPageRank;
	}



	public void setIncomingPageRank(double incomingPageRank) {
		this.incomingPageRank = incomingPageRank;
	}



	public boolean isSameBlock() {
		return isSameBlock;
	}



	public void setSameBlock(boolean isSameBlock) {
		this.isSameBlock = isSameBlock;
	}
	
	

}
