package com.yullage.bigdata2013;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NumberSortReducer extends MapReduceBase implements
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	public void reduce(IntWritable key, Iterator<IntWritable> values,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {
		while (values.hasNext()) {
			output.collect(key, values.next());
		}
	}
}
