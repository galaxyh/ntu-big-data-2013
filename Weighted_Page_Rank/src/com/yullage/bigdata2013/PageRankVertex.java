package com.yullage.bigdata2013;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

/**
 * @author Yu-chun Huang
 * @version 1.0b
 */
public class PageRankVertex extends
		Vertex<Text, NullWritable, PageRankWritable> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hama.graph.VertexInterface#compute(java.lang.Iterable)
	 */
	@Override
	public void compute(Iterable<PageRankWritable> messages) throws IOException {

		if (this.getSuperstepCount() == 0) {
			// initialize this vertex to 1/count of global vertices
			// in this graph.
			PageRankWritable vertexContent = new PageRankWritable();
			vertexContent.setRank(1.0 / this.getNumVertices());
			this.setValue(vertexContent);
			// Broadcast this vertex ID for neighbors
			// to calculate in and out edge counts.
			broadcastVertexId();
			return;

		} else if (getSuperstepCount() == 1) {
			// Calculate in and out edge counts.
			// Then send these information back to senders.
			sendInOutEdgeCounts(messages);
			return;

		} else if (getSuperstepCount() == 2) {
			// Calculate weight for each neighbor and
			// then continue to next super step.
			calculateWeight(messages);
			return;

		} else if (getSuperstepCount() > 3) {
			// Calculate page rank.
			double sum = 0;
			for (PageRankWritable msg : messages) {
				sum += msg.getRank().get();
			}

			double alpha = 1.0d - 0.85;
			this.getValue().setRank(alpha + (sum * 0.85d));
		}

		if (getSuperstepCount() >= 3) {
			if (getSuperstepCount() < getMaxIteration()) {
				// Send out new page rank to all neighbors.
				sendNewRank();
			} else {
				System.out.println("Vertex = " + getVertexID() + "; Rank = "
						+ getValue().getRank());
				voteToHalt();
			}
		}
	}

	/**
	 * Broadcast this vertex ID for neighbors to calculate in and out edge
	 * counts.
	 * 
	 * @throws IOException
	 */
	private void broadcastVertexId() throws IOException {
		PageRankWritable msg = new PageRankWritable();
		msg.setSenderId(getVertexID());
		sendMessageToNeighbors(msg);
	}

	/**
	 * Calculate in and out edge counts. Send these information back to senders.
	 * 
	 * @param messages
	 * @throws IOException
	 */
	private void sendInOutEdgeCounts(Iterable<PageRankWritable> messages)
			throws IOException {
		// Receive vertex IDs from all sender.
		List<Text> vertexIdList = new ArrayList<Text>();
		for (PageRankWritable msg : messages) {
			vertexIdList.add(msg.getSenderId());
		}

		// Send incoming and outgoing edge counts back to senders.
		PageRankWritable msg = new PageRankWritable();
		msg.setSenderId(getVertexID());
		msg.setInEdgeCount(vertexIdList.size());
		msg.setOutEdgeCount(getEdges().size());

		for (Text id : vertexIdList) {
			sendMessage(id, msg);
		}
	}

	/**
	 * Calculate weight for each destination vertex.
	 * 
	 * @param messages
	 */
	private void calculateWeight(Iterable<PageRankWritable> messages) {
		long totalInCount = 0;
		long totalOutCount = 0;

		// Because iterator can not go back to the first record,
		// so we need to new another Map to store the in/out edge counts.
		Map<Text, long[]> edgeCountMap = new HashMap<Text, long[]>();
		for (PageRankWritable msg : messages) {
			totalInCount += msg.getInEdgeCount().get();
			totalOutCount += msg.getOutEdgeCount().get();

			// Store edge counts.
			long[] edgeCounts = new long[2];
			edgeCounts[0] = msg.getInEdgeCount().get();
			edgeCounts[1] = msg.getOutEdgeCount().get();
			edgeCountMap.put(msg.getSenderId(), edgeCounts);
		}

		// Calculate weight for each destination vertex.
		MapWritable weightMap = new MapWritable();
		for (Entry<Text, long[]> entry : edgeCountMap.entrySet()) {
			Text key = entry.getKey();
			long[] value = entry.getValue();
			double weight = (value[0] / (double) totalInCount)
					* (value[1] / (double) totalOutCount);
			weightMap.put(key, new DoubleWritable(weight));
		}

		getValue().setWeightMap(weightMap);
	}

	/**
	 * Send new rank to neighbors.
	 * 
	 * @throws IOException
	 */
	private void sendNewRank() throws IOException {
		for (Edge<Text, NullWritable> edge : getEdges()) {
			double thisRank = getValue().getRank().get();
			double destWeight = 0;
			try {
				destWeight = getValue()
						.getWeight(edge.getDestinationVertexID()).get();
			} catch (Exception e) {
				System.err
						.println("[ WARNING !!! ] Cant's find the weight for vertex ID = "
								+ edge.getDestinationVertexID());
				continue;
			}

			double outRank = thisRank * destWeight;
			System.err
					.println("ST" + getSuperstepCount() + "SNR OR:" + outRank);
			PageRankWritable msg = new PageRankWritable();
			msg.setRank(outRank);
			sendMessage(edge, msg);
		}
	}

}