package com.yullage.bigdata2013;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

public class PageRankVertex extends Vertex<Text, NullWritable, PageRankWritable> {

	public static double DAMPING_FACTOR = 0.85;
	private static int SETUP_STEPS = 3;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hama.graph.Vertex#setup(org.apache.hadoop.conf.Configuration)
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
		if (getSuperstepCount() <= 3) {
			System.out.println("Vertex = " + this.getVertexID() + " Superstep = " + this.getSuperstepCount());
		}

		if (this.getSuperstepCount() == 0) {
			// initialize this vertex to 1/count of global vertices in this graph.
			PageRankWritable vertexContent = new PageRankWritable();
			vertexContent.setRank(1.0 / this.getNumVertices());
			this.setValue(vertexContent);

			// Broadcast this vertex ID for neighbors to calculate in and out edge counts.
			broadcastVertexId();

		} else if (this.getSuperstepCount() == 1) {
			// Calculate in and out edge counts. Then send these information back to senders.
			sendInOutEdgeCounts(messages);
		} else if (this.getSuperstepCount() == 2) {
			// Calculate weight for each neighbor.
			calculateWeight(messages);

			// Just continue to next super step.
			return;

		} else if (this.getSuperstepCount() >= 3) {
			double sum = 0;
			for (PageRankWritable msg : messages) {
				sum += msg.getRank().get();
			}

			double alpha = (1.0d - DAMPING_FACTOR) / this.getNumVertices();
			this.getValue().setRank(alpha + (sum * DAMPING_FACTOR));
		}

		System.out.println("Rank = " + this.getValue().getRank().toString());

		if (this.getSuperstepCount() < this.getMaxIteration() + SETUP_STEPS) {
			sendNewRank();
		} else {
			this.voteToHalt();
			return;
		}
	}

	/**
	 * Broadcast this vertex ID for neighbors to calculate in and out edge counts.
	 * 
	 * @throws IOException
	 */
	private void broadcastVertexId() throws IOException {
		System.out.println("broadcast: " + getVertexID().toString());

		for (Edge<Text, NullWritable> e : this.getEdges()) {
			System.out.println("dst:" + e.getDestinationVertexID().toString());
		}

		PageRankWritable msg = new PageRankWritable();
		msg.setSenderId(getVertexID());

		for (Edge<Text, NullWritable> edge : this.getEdges()) {
			sendMessage(edge, msg);
		}
		// sendMessageToNeighbors(msg);
	}

	/**
	 * Calculate in and out edge counts. Send these information back to senders.
	 * 
	 * @param messages
	 * @throws IOException
	 */
	private void sendInOutEdgeCounts(Iterable<PageRankWritable> messages) throws IOException {
		// Receive vertex IDs from all sender.
		List<Text> vertexIdList = new ArrayList<Text>();
		for (PageRankWritable msg : messages) {
			System.out.println("Id = " + getVertexID() + " Sender = " + msg.getSenderId());
			vertexIdList.add(msg.getSenderId());
		}

		// Send incoming and outgoing edge counts back to senders.
		PageRankWritable msg = new PageRankWritable();
		msg.setSenderId(getVertexID());
		msg.setInEdgeCount(vertexIdList.size());
		msg.setOutEdgeCount(getEdges().size());

		System.out.println("Id = " + getVertexID() + " In EC = " + vertexIdList.size() + " Out EC = "
		        + getEdges().size());

		for (Text id : vertexIdList) {
			sendMessage(id, msg);
		}
	}

	private void calculateWeight(Iterable<PageRankWritable> messages) {

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