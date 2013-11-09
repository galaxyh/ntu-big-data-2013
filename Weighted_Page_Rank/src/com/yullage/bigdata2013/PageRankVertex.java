package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Vertex;

public class PageRankVertex extends Vertex<Text, NullWritable, DoubleWritable> {

	static double DAMPING_FACTOR = 0.85;
	static int MAX_SUPERSTEP = 30;

	@Override
	public void setup(Configuration conf) {
		String val = conf.get("hama.pagerank.alpha");
		if (val != null) {
			DAMPING_FACTOR = Double.parseDouble(val);
		}
		val = conf.get("hama.pagerank.maxSuperstep");
		if (val != null) {
			MAX_SUPERSTEP = Integer.parseInt(val);
		}
	}

	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
		System.out.println("Vertex " + this.getVertexID() + " Superstep ="
				+ this.getSuperstepCount());

		// initialize this vertex to 1 / count of global vertices in this
		// graph
		if (this.getSuperstepCount() == 0) {
			this.setValue(new DoubleWritable(1.0 / this.getNumVertices()));
		} else if (this.getSuperstepCount() >= 1) {
			double sum = 0;
			for (DoubleWritable msg : messages) {
				sum += msg.get();
			}
			double alpha = (1.0d - DAMPING_FACTOR) / this.getNumVertices();
			this.setValue(new DoubleWritable(alpha + (sum * DAMPING_FACTOR)));
		}

		if (this.getSuperstepCount() == MAX_SUPERSTEP) {
			this.voteToHalt();
			return;
		}

		// Send a new rank to neighbors.
		sendMessageToNeighbors(new DoubleWritable(this.getValue().get()
				/ this.getEdges().size()));
	}

}