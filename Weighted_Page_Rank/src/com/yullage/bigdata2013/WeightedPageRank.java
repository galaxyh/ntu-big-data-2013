package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.GraphJob;

public class WeightedPageRank {

	private static GraphJob createJob(String[] args, HamaConfiguration conf)
			throws IOException {
		GraphJob graphJob = new GraphJob(conf, WeightedPageRank.class);
		graphJob.setJobName("Weighted page rank");

		graphJob.setVertexClass(PageRankVertex.class);
		graphJob.setMaxIteration(Integer.parseInt(args[0]));
		graphJob.setInputPath(new Path(args[1]));
		graphJob.setOutputPath(new Path(args[2]));

		// set the defaults
		//graphJob.set(hama., value);
		graphJob.set("hama.pagerank.alpha", "0.85");
		// reference vertices to itself, because we don't have a dangling node
		// contribution here
		graphJob.set("hama.graph.self.ref", "true");

		// Vertex reader
		graphJob.setVertexInputReaderClass(PageRankDataReader.class);

		graphJob.setVertexIDClass(Text.class);
		graphJob.setVertexValueClass(DoubleWritable.class);
		graphJob.setEdgeValueClass(NullWritable.class);

		graphJob.setInputFormat(TextInputFormat.class);

		graphJob.setPartitioner(HashPartitioner.class);
		graphJob.setOutputFormat(TextOutputFormat.class);
		graphJob.setOutputKeyClass(Text.class);
		graphJob.setOutputValueClass(DoubleWritable.class);

		return graphJob;
	}

	private static void printUsage() {
		System.out.println("Usage: <iterations> <input> <output>");
		System.exit(-1);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length < 3) {
			printUsage();
		}

		HamaConfiguration conf = new HamaConfiguration(new Configuration());
		GraphJob graphJob = createJob(args, conf);

		long startTime = System.currentTimeMillis();
		if (graphJob.waitForCompletion(true)) {
			System.out.println("Job Finished in "
					+ (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");
		}
	}
}