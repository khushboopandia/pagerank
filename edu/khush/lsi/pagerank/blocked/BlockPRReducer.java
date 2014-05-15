package edu.khush.lsi.pagerank.blocked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.lsi.project2.pagerank.simplemr.PageRankSimple;

public class BlockPRReducer extends
		Reducer<LongWritable, NodeOrEdge, LongWritable, Node> {

	public static double alpha = 0.85;
	private HashMap<Long, Double> currentPrMap = new HashMap<Long, Double>();

	public void reduce(LongWritable blockId, Iterable<NodeOrEdge> Ns,
			Context context) throws IOException, InterruptedException {

		// List of all nodes in the block
		ArrayList<Long> listOfNodes = new ArrayList<Long>();

		currentPrMap.clear();

		// Get all old page ranks
		HashMap<Long, Node> nodeDataMap = new HashMap<Long, Node>();
		HashMap<Long, ArrayList<Long>> sameBlockEdges = new HashMap<Long, ArrayList<Long>>();
		HashMap<Long, Double> outsideBlockEdges = new HashMap<Long, Double>();
		NodeOrEdge temp = null;
		Iterator<NodeOrEdge> iterator = Ns.iterator();
		ArrayList<Long> tempEdgesList = null;
		Double tempBC = 0.0;
		long maxNodeId = -1;

		while (iterator.hasNext()) {

			temp = iterator.next();

			// If it is a node, do the following:
			// 1. Add it to node data map
			// 2. Add it to the list of nodes in the block
			// 3.Add its page rank to the current page rank map
			// 4. Keep track of max node id in the block
			if (temp.isNode()) {

				currentPrMap.put(temp.getNode().getNodeID(), temp.getNode()
						.getPageRank());
				nodeDataMap.put(temp.getNode().getNodeID(), temp.getNode());
				listOfNodes.add(temp.getNode().getNodeID());

				if (temp.getNode().getNodeID() > maxNodeId) {
					maxNodeId = temp.getNode().getNodeID();
				}

				// If it is an edge, check if it same block or incoming from
				// another block
			} else {

				// If it is same block edge, do the below:
				// 1. Add it to same block edges map where key is destination
				// node and value if list of all nodes which have edges to the
				// dest node.
				if (temp.getEdge().isSameBlock) {
					if (!sameBlockEdges.containsKey(temp.getEdge()
							.getDestNode())) {
						tempEdgesList = new ArrayList<Long>();

					}

					else {
						tempEdgesList = sameBlockEdges.get(temp.getEdge()
								.getDestNode());

					}

					tempEdgesList.add(temp.getEdge().getSourceNode());
					sameBlockEdges.put(temp.getEdge().getDestNode(),
							tempEdgesList);

				}
				// If it is same block edge, do the below:
				// Add it to outsideBlockEdges map where key is destination node
				// and value if total page rank it is receiving form nodes
				// outside the block.
				else {

					if (!outsideBlockEdges.containsKey(temp.getEdge()
							.getDestNode())) {
						tempBC = 0.0;
					}

					else {
						tempBC = outsideBlockEdges.get(temp.getEdge()
								.getDestNode());

					}

					tempBC += temp.getEdge().getIncomingPageRank();
					outsideBlockEdges.put(temp.getEdge().getDestNode(), tempBC);

				}

			}

		}

		// Run until convergence with threshold of 0.001

		double residualError = Double.MAX_VALUE;
		int i = 0;
		do {
			i++;
			residualError = iterateBlockOnceJacobi(currentPrMap, listOfNodes,
					nodeDataMap, sameBlockEdges, outsideBlockEdges);

			// Uncomment the below line and comment the prev line to run Gauss
			// Seidel instead of Jacobi
			// residualError = iterateBlockOnceJacobi(currentPrMap, listOfNodes,
			// nodeDataMap,
			// sameBlockEdges, outsideBlockEdges);

		} while (residualError > 0.001);

		// Add below to the while condition to run only upto 5 iteartions of
		// page rank.
		// i < 5 &&

		// compute the ultimate residual error for each node in this block
		residualError = 0.0f;
		for (Long v : listOfNodes) {
			Node node = nodeDataMap.get(v);
			residualError += Math.abs(node.getPageRank() - currentPrMap.get(v))
					/ currentPrMap.get(v);
		}
		residualError = residualError / listOfNodes.size();
		long finalResidualValue = (long) Math.floor(residualError
				* PageRankSimple.precision);
		context.getCounter(PageRankSimple.Counters.RESIDUAL_ERROR).increment(
				finalResidualValue);
		context.getCounter(PageRankSimple.Counters.NO_OF_ITERS).increment(i);

		// Emit the node and their latest page rank after one iteration of the
		// reducer.
		for (Long nodeId : listOfNodes) {
			Node n = nodeDataMap.get(nodeId);
			n.setPageRank(currentPrMap.get(nodeId));
			context.write(new LongWritable(nodeId), n);
		}

		

		cleanup(context);

	}

	// Jacobi
	public Double iterateBlockOnceJacobi(HashMap<Long, Double> currentPrMap,
			ArrayList<Long> listOfNodes, HashMap<Long, Node> nodeDataMap,
			HashMap<Long, ArrayList<Long>> sameBlockEdges,
			HashMap<Long, Double> outsideBlockEdges) {

		Double newPR = 0.0;
		Double resErr = 0.0;
		Double prevPR = 0.0;
		HashMap<Long, Double> tempPr = new HashMap<Long, Double>();

		// Loop through all nodes in the block
		for (Long nodeId : listOfNodes) {
			newPR = 0.0;
			prevPR = currentPrMap.get(nodeId);

			// Add page rank incoming to the current node form all other nodes
			// in the block
			if (sameBlockEdges.containsKey(nodeId)) {
				for (Long adjNodeId : sameBlockEdges.get(nodeId)) {
					newPR += currentPrMap.get(adjNodeId)
							/ nodeDataMap.get(adjNodeId).getOutgoingEdges()
									.size();
				}
			}

			// Add page rank incoming to the current node form all other nodes
			// outside the block
			if (outsideBlockEdges.containsKey(nodeId))
				newPR += outsideBlockEdges.get(nodeId);
			newPR = (alpha * newPR) + ((1 - alpha) / new Double(685230));
			tempPr.put(nodeId, newPR);
			resErr += Math.abs(prevPR - newPR) / newPR;

		}

		for (Long nodeId : listOfNodes) {
			currentPrMap.put(nodeId, tempPr.get(nodeId));
		}

		resErr = resErr / listOfNodes.size();
		return resErr;

	}

	// Gauss Seidel
	public Double iterateBlockOnceGS(HashMap<Long, Double> currentPrMap,
			ArrayList<Long> listOfNodes, HashMap<Long, Node> nodeDataMap,
			HashMap<Long, ArrayList<Long>> sameBlockEdges,
			HashMap<Long, Double> outsideBlockEdges) {

		Double newPR = 0.0;
		Double resErr = 0.0;
		Double prevPR = 0.0;
		HashMap<Long, Double> tempPr = new HashMap<Long, Double>();

		// Loop through all nodes in the block
		for (Long nodeId : listOfNodes) {
			newPR = 0.0;
			prevPR = currentPrMap.get(nodeId);

			// Add page rank incoming to the current node form all other nodes
			// in the block
			if (sameBlockEdges.containsKey(nodeId)) {
				for (Long adjNodeId : sameBlockEdges.get(nodeId)) {
					newPR += currentPrMap.get(adjNodeId)
							/ nodeDataMap.get(adjNodeId).getOutgoingEdges()
									.size();
				}
			}

			// Add page rank incoming to the current node form all other nodes
			// outside the block
			if (outsideBlockEdges.containsKey(nodeId))
				newPR += outsideBlockEdges.get(nodeId);
			newPR = (alpha * newPR) + ((1 - alpha) / new Double(685230));
			currentPrMap.put(nodeId, newPR);
			resErr += Math.abs(prevPR - newPR) / newPR;

		}

		resErr = resErr / listOfNodes.size();
		return resErr;

	}

}
