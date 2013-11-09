package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Vertex;

public class PageRankVertex extends Vertex<Text, NullWritable, PageRankWritable> {

	public static double DAMPING_FACTOR = 0.85;
	private static int SETUP_STEPS = 2;

	@Override
	public void setup(Configuration conf) {
		String val = conf.get("hama.pagerank.alpha");
		if (val != null) {
			DAMPING_FACTOR = Double.parseDouble(val);
		}
	}

	@Override
	public void compute(Iterable<PageRankWritable> messages) throws IOException {
		System.out.println("Vertex = " + this.getVertexID() + " Superstep = " + this.getSuperstepCount());

		// initialize this vertex to 1 / count of global vertices in this
		// graph
		if (this.getSuperstepCount() == 0) {
			PageRankWritable prWritable = new PageRankWritable();
			prWritable.setRank(1.0 / this.getNumVertices());
			this.setValue(prWritable);

		} else if (this.getSuperstepCount() >= 1) {
			double sum = 0;
			for (PageRankWritable msg : messages) {
				sum += msg.getRank().get();
			}

			double alpha = (1.0d - DAMPING_FACTOR) / this.getNumVertices();
			this.getValue().setRank(alpha + (sum * DAMPING_FACTOR));
		}

		System.out.println("Rank = " + this.getValue().getRank().toString());

		if (this.getSuperstepCount() < this.getMaxIteration() + SETUP_STEPS) {
			// Send a new rank to neighbors.
			double outRank = this.getValue().getRank().get() / this.getEdges().size();
			PageRankWritable msg = new PageRankWritable();
			msg.setRank(outRank);
			sendMessageToNeighbors(msg);
		} else {
			this.voteToHalt();
			return;
		}
	}

}