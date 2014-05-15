package edu.khush.lsi.pagerank.blocked;

import java.io.IOException;
import java.io.DataOutputStream;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class NodeRecordWriter extends RecordWriter<LongWritable, Node> {

	private DataOutputStream out;

	public NodeRecordWriter(DataOutputStream out) throws IOException {
		this.out = out;
	}

	public synchronized void write(LongWritable key, Node value)
			throws IOException {
		boolean keyNull = key == null;
		boolean valueNull = value == null;

		if (valueNull) { // Can't write a Null value
			return;
		}
		if (keyNull) {
			write(new LongWritable(value.nodeID), value); // If we have a null
															// key, then just
															// use the Node's
															// nodeid
		}

		String nodeRep = key.toString() + " " + " " + value.getBlockID() + " "
				+ value.getPageRank() + " "; 
		
		String outGoing = "";
		for (Long n : value.outgoingEdges) {
											
			outGoing += n.toString() + " ";
		}
		if (!outGoing.equals(""))
			nodeRep += outGoing.substring(0, outGoing.length() - 1); 
		out.writeBytes(nodeRep.trim() + "\n");
	}

	public synchronized void close(TaskAttemptContext ctxt) throws IOException {
		out.close();
	}
}