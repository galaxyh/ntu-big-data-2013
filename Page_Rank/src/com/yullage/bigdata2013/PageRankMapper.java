package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * A page rank mapper implementation.
 * 
 * @author Steven Huang
 * @version 1.0b
 * 
 */
public class PageRankMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String[] tokens = line.split("\\s+");
		int tokenCount = tokens.length;

		// Skip invalid lines and pages without any out link.
		if (tokenCount <= 2) {
			return;
		}

		// Rank and link count.
		String outLinks = "";
		for (int i = 2; i < tokenCount; i++) {
			outLinks = outLinks + " " + tokens[i];

			Text k = new Text(tokens[i]);
			Text v = new Text(tokens[1] + " "
					+ Integer.toString(tokenCount - 2));
			output.collect(k, v);
		}

		// Output the original out links starting with OUT_LINK_FLAG.
		output.collect(new Text(tokens[0]), new Text(PageRank.OUT_LINK_FLAG
				+ outLinks));
	}

}
