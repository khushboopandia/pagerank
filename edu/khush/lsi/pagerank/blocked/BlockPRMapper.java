package edu.khush.lsi.pagerank.blocked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class BlockPRMapper extends
		Mapper<LongWritable, Node, LongWritable, NodeOrEdge> {

	

	public void map(LongWritable key, Node value, Context context)
			throws IOException, InterruptedException {
		
		// Get block id
		long blockId =lookupBlockID(value.getNodeID());
		//Uncomment and use this instead of previous line when running for random partitioning input 
		//long blockId=PageRankJob.nodeBlockIdMap.get(value.getNodeID());
		value.setBlockID(blockId);

		// Emit the block id and node
		context.write(new LongWritable(blockId), new NodeOrEdge(value));

		
		//Emit the edges
		if (value.outgoingEdges.size() != 0) {
			double outgoingPR = value.getPageRank()
					/ value.outgoingEdges.size();

			Iterator<Long> adjacentNodesIterator = value.iterator();
			while (adjacentNodesIterator.hasNext()) {
				Long adjNodeId = adjacentNodesIterator.next();
				Long adjBlockId=lookupBlockID(adjNodeId);
				
				//Uncomment and use this  when running for random partitioning input 
				//if(adjBlockId==null)
					//adjBlockId=new Long(-1);
				
				//Emit edges coming to this block id from other blocks
				if( blockId!=adjBlockId)
				{
					Edge edge=new Edge(value.getNodeID(),adjNodeId,outgoingPR,false);
					context.write(
							new LongWritable(adjBlockId),
							new NodeOrEdge(edge));
				}
				//Emit edges within the block
				else
				{
					Edge edge=new Edge(value.getNodeID(),adjNodeId,outgoingPR,true);
					context.write(
							new LongWritable(adjBlockId),
							new NodeOrEdge(edge));
				}
				
				
			}

		}

	}

	
	
	// lookup the block ID in a hardcoded list based on the node ID
	public static long lookupBlockID(long nodeID) {
		int partitionSize = 10000;
		int[] blockBoundaries = { 0, 10328, 20373, 30629, 40645,
				50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945,
				130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004,
				212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473,
				293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929,
				374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489,
				454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
				534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
				616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230 };

		
		/*Temporarily done linearly. Can be improved by converting to a binary ceil search*/
		long blockID = (long) Math.floor(nodeID / partitionSize);
		long testBoundary = blockBoundaries[(int)blockID];
		if (nodeID < testBoundary) {
			blockID--;
		}
		
		return blockID;
	}
}
