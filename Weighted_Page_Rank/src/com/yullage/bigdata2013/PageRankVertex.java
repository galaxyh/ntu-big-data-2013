package com.yullage.bigdata2013;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Vertex;

public class PageRankVertex extends
		Vertex<Text, NullWritable, PageRankWritable> {

	public static double DAMPING_FACTOR = 0.85;
	public static int SETUP_STEPS = 3;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hama.graph.Vertex#setup(org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void setup(Configuration conf) {
		String val = conf.get("hama.pagerank.alpha");
		if (val != null) {
			DAMPING_FACTOR = Double.parseDouble(val);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hama.graph.VertexInterface#compute(java.lang.Iterable)
	 */
	@Override
	public void compute(Iterable<PageRankWritable> messages) throws IOException {
		System.out.println("Vertex = " + getVertexID() + "; Superstep = "
				+ getSuperstepCount());

		if (this.getSuperstepCount() == 0) {
			// initialize this vertex to 1/count of global vertices in this
			// graph.
			PageRankWritable vertexContent = new PageRankWritable();
			vertexContent.setRank(1.0 / this.getNumVertices());
			this.setValue(vertexContent);

			// Broadcast this vertex ID for neighbors to calculate in and out
			// edge counts.
			broadcastVertexId();

		} else if (getSuperstepCount() == 1) {
			// Calculate in and out edge counts. Then send these information
			// back to senders.
			sendInOutEdgeCounts(messages);
		} else if (getSuperstepCount() == 2) {
			// Calculate weight for each neighbor and then continue to next
			// super step.
			calculateWeight(messages);
			return;

		} else if (getSuperstepCount() > 3) {
			double sum = 0;
			for (PageRankWritable msg : messages) {
				sum += msg.getRank().get();
			}

			double alpha = 1.0d - DAMPING_FACTOR;
			this.getValue().setRank(alpha + (sum * DAMPING_FACTOR));
		}

		System.out.println("Rank = " + this.getValue().getRank().toString());

		if (getSuperstepCount() >= SETUP_STEPS) {
			if (getSuperstepCount() < getMaxIteration()) {
				sendNewRank();
			} else {
				voteToHalt();
				return;
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

		System.out.println("Id = " + getVertexID() + "; In edge count = "
				+ vertexIdList.size() + "; Out edge count = "
				+ getEdges().size());

		for (Text id : vertexIdList) {
			sendMessage(id, msg);
		}
	}

	private void calculateWeight(Iterable<PageRankWritable> messages) {
		long totalInCount = 0;
		long totalOutCount = 0;
		for (PageRankWritable msg : messages) {
			totalInCount += msg.getInEdgeCount().get();
			totalOutCount += msg.getOutEdgeCount().get();
		}

		Map<Text, DoubleWritable> map = new HashMap<Text, DoubleWritable>();
		for (PageRankWritable msg : messages) {
			double weight = (msg.getInEdgeCount().get() / totalInCount)
					* (msg.getOutEdgeCount().get() / totalOutCount);
			map.put(msg.getSenderId(), new DoubleWritable(weight));
		}

		MapWritable weightMap = new MapWritable();
		weightMap.putAll(map);
		this.getValue().setWeightMap(weightMap);
	}

	/**
	 * Send new rank to neighbors.
	 * 
	 * @throws IOException
	 */
	private void sendNewRank() throws IOException {
		double outRank = this.getValue().getRank().get() / getEdges().size();
		PageRankWritable msg = new PageRankWritable();
		msg.setRank(outRank);
		sendMessageToNeighbors(msg);
	}

}