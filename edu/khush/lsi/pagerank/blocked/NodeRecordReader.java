package edu.khush.lsi.pagerank.blocked;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

//Based off of the code at http://developer.yahoo.com/hadoop/tutorial/module5.html
public class NodeRecordReader extends RecordReader<LongWritable, Node> {

	private LineRecordReader lineReader; // Does the actual reading for us
	private LongWritable lineKey; // The key from lineReader
									// Contains the line number we're reading
									// from
	private Text lineValue; // The value from lineReader
							// Contains the text from the line we're reading
	private LongWritable curKey; // The current key of our NodeRecordReader, the
									// current Node's nodeid
	private Node curVal; // The current value of our NodeRecordReader, the
							// current Node
	private long end = 0, start = 0, pos = 0, maxLineLength; // Line tracking
																// variables

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// To initialize, create and initialize our lineReader
		lineReader = new LineRecordReader();
		lineReader.initialize(genericSplit, context);
	}

	public boolean nextKeyValue() throws IOException {
		if (!lineReader.nextKeyValue()) {// If we're out of lines, we're done
			return false;
		}

		// get the correct current key and value
		lineKey = lineReader.getCurrentKey();
		lineValue = lineReader.getCurrentValue();

		// The line needs to be split into 4 pieces: the node id, the block id,
		// the current PageRank, and the optional list of outgoing links
		String[] pieces = lineValue.toString().split("\\s+");

		// Get the outgoing edges
		ArrayList<Long> outs = new ArrayList<Long>();
		if (pieces.length > 3) {

			for (int i = 3; i < pieces.length; i++) {
				outs.add(Long.parseLong(pieces[i].trim()));
			}

		} else {
			outs = new ArrayList<Long>();
		}

		long nodeId;// attempt to parse the nodeid
		try {
			nodeId = Long.parseLong(pieces[0].trim());
		} catch (NumberFormatException nfe) {
			throw new IOException(" Error parsing integer in record: "
					+ nfe.toString());
		}

		// attempt to parse the block id
		long blockid;
		try {
			blockid = Long.parseLong(pieces[1].trim());
		} catch (NumberFormatException nfe) {
			throw new IOException(" Error parsing integer in record: "
					+ nfe.toString());
		}

		// attempt to parse the current PageRank
		double pageRank;
		try {
			pageRank = Double.parseDouble(pieces[2]);
		} catch (NumberFormatException nfe) {
			throw new IOException(" Error parsing double in record: "
					+ nfe.toString());
		}

		// Set key and value pairs
		curKey = new LongWritable(nodeId);
		curVal = new Node(nodeId, outs);
		curVal.setPageRank(pageRank);
		curVal.setBlockID(blockid);

		return true;

	}

	public LongWritable getCurrentKey() {
		return curKey;
	}

	public Node getCurrentValue() {
		return curVal;
	}

	public void close() throws IOException {
		lineReader.close();
	}

	public float getProgress() throws IOException {
		return lineReader.getProgress();
	}
}