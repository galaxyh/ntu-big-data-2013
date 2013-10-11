package com.yullage.bigdata2013;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NumberSortMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private IntWritable number = new IntWritable();

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			number.set(Integer.parseInt(tokenizer.nextToken()));
			output.collect(number, one);
		}
	}

}
