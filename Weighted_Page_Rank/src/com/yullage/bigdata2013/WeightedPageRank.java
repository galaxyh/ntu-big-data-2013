package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.GraphJob;

public class WeightedPageRank {

	private static GraphJob createJob(String[] args, HamaConfiguration conf) throws IOException {
		GraphJob graphJob = new GraphJob(conf, WeightedPageRank.class);
		graphJob.setJobName("Weighted page rank");

		graphJob.setVertexClass(PageRankVertex.class);
		graphJob.setInputPath(new Path(args[1]));
		graphJob.setOutputPath(new Path(args[2]));

		// set the defaults
		graphJob.setMaxIteration(Integer.parseInt(args[0]) + 3);
		graphJob.set("hama.pagerank.alpha", "0.85");

		// Vertex reader
		graphJob.setVertexInputReaderClass(PageRankDataReader.class);

		graphJob.setVertexIDClass(Text.class);
		graphJob.setVertexValueClass(PageRankWritable.class);
		graphJob.setEdgeValueClass(NullWritable.class);

		graphJob.setInputFormat(TextInputFormat.class);
		graphJob.setInputKeyClass(LongWritable.class);
		graphJob.setInputValueClass(Text.class);

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

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length < 3) {
			printUsage();
		}

		HamaConfiguration conf = new HamaConfiguration(new Configuration());
		GraphJob graphJob = createJob(args, conf);

		long startTime = System.currentTimeMillis();
		if (graphJob.waitForCompletion(true)) {
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
	}
}