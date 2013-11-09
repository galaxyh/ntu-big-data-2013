package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Vertex;

public class PageRankVertex extends Vertex<Text, NullWritable, MapWritable> {

	static double DAMPING_FACTOR = 0.85;
	private static int SETUP_STEPS = 2;

	@Override
	public void setup(Configuration conf) {
		String val = conf.get("hama.pagerank.alpha");
		if (val != null) {
			DAMPING_FACTOR = Double.parseDouble(val);
		}
	}

	@Override
	public void compute(Iterable<MapWritable> messages) throws IOException {
		System.out.println("Vertex " + this.getVertexID() + " Superstep =" + this.getSuperstepCount());

		// initialize this vertex to 1 / count of global vertices in this
		// graph
		if (this.getSuperstepCount() == 0) {
			MapWritable vertexValue = new MapWritable();
			vertexValue.put(new Text("Rank"), new DoubleWritable(1.0 / this.getNumVertices()));
			this.setValue(vertexValue);
		} else if (this.getSuperstepCount() >= 1) {
			double sum = 0;
			for (MapWritable msg : messages) {
				DoubleWritable rank = (DoubleWritable) msg.get("RANK");
				sum += Double.parseDouble(rank.toString());
			}

			double alpha = (1.0d - DAMPING_FACTOR) / this.getNumVertices();
			this.getValue().put(new Text("Rank"), new DoubleWritable(alpha + (sum * DAMPING_FACTOR)));
		}

		if (this.getSuperstepCount() < this.getMaxIteration() + SETUP_STEPS) {
			// Send a new rank to neighbors.
			Double outRank = Double.parseDouble(this.getValue().get("RANK").toString()) / this.getEdges().size();
			MapWritable messageContent = new MapWritable();
			messageContent.put(new Text("Rank"), new Text(outRank.toString()));
			sendMessageToNeighbors(messageContent);
		} else {
			this.voteToHalt();
			return;
		}
	}

}