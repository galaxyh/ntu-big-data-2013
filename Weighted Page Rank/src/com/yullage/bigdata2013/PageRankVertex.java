package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Vertex;

public class PageRankVertex extends Vertex<Text, NullWritable, DoubleWritable> {

	static double DAMPING_FACTOR = 0.85;
	static double MAXIMUM_CONVERGENCE_ERROR = 0.001;

	@Override
	public void setup(Configuration conf) {
		String val = conf.get("hama.pagerank.alpha");
		if (val != null) {
			DAMPING_FACTOR = Double.parseDouble(val);
		}
		val = conf.get("hama.graph.max.convergence.error");
		if (val != null) {
			MAXIMUM_CONVERGENCE_ERROR = Double.parseDouble(val);
		}
	}

	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
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

		// if we have not reached our global error yet, then proceed.
		DoubleWritable globalError = getLastAggregatedValue(0);
		if (globalError != null && this.getSuperstepCount() > 2
				&& MAXIMUM_CONVERGENCE_ERROR > globalError.get()) {
			voteToHalt();
			return;
		}

		// in each superstep we are going to send a new rank to our
		// neighbours
		sendMessageToNeighbors(new DoubleWritable(this.getValue().get()
				/ this.getEdges().size()));
	}

}
