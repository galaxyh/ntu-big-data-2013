package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 * A weighted page rank implementation with Hama.
 * 
 * @author Steven Huang
 * @version 1.0b
 * 
 */
public class WeightedPageRank {

	private static GraphJob createJob(String[] args, HamaConfiguration conf)
			throws IOException {
		GraphJob pageJob = new GraphJob(conf, WeightedPageRank.class);
		pageJob.setJobName("Pagerank");

		pageJob.setVertexClass(PageRankVertex.class);
		pageJob.setInputPath(new Path(args[0]));
		pageJob.setOutputPath(new Path(args[1]));

		// set the defaults
		pageJob.setMaxIteration(30);
		pageJob.set("hama.pagerank.alpha", "0.85");
		// reference vertices to itself, because we don't have a dangling node
		// contribution here
		pageJob.set("hama.graph.self.ref", "true");
		pageJob.set("hama.graph.max.convergence.error", "0.001");

		if (args.length == 3) {
			pageJob.setNumBspTask(Integer.parseInt(args[2]));
		}

		// error
		pageJob.setAggregatorClass(AverageAggregator.class);

		// Vertex reader
		pageJob.setVertexInputReaderClass(PageRankSeqReader.class);

		pageJob.setVertexIDClass(Text.class);
		pageJob.setVertexValueClass(DoubleWritable.class);
		pageJob.setEdgeValueClass(NullWritable.class);

		pageJob.setInputFormat(SequenceFileInputFormat.class);

		pageJob.setPartitioner(HashPartitioner.class);
		pageJob.setOutputFormat(TextOutputFormat.class);
		pageJob.setOutputKeyClass(Text.class);
		pageJob.setOutputValueClass(DoubleWritable.class);
		return pageJob;
	}

	private static void printUsage() {
		System.out.println("Usage: <input> <output> [tasks]");
		System.exit(-1);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length < 2)
			printUsage();

		HamaConfiguration conf = new HamaConfiguration(new Configuration());
		GraphJob pageJob = createJob(args, conf);

		long startTime = System.currentTimeMillis();
		if (pageJob.waitForCompletion(true)) {
			System.out.println("Job Finished in "
					+ (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");
		}
	}
}
