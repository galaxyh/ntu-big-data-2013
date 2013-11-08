package com.yullage.bigdata2013;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * An integer sort reducer implementation.
 * 
 * @author Steven Huang
 * @version 1.0b
 * 
 */
public class IntegerSortReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text> {
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		while (values.hasNext()) {
			output.collect(key, new Text(""));
			values.next();
		}
	}
}
