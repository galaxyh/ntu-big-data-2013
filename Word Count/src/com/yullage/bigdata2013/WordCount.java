package com.yullage.bigdata2013;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf jobConf = new JobConf(WordCount.class);

		// Specify job name
		jobConf.setJobName("Word count");

		// Specify output types
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);

		// Specify mapper and reducer
		jobConf.setMapperClass(WordCountMapper.class);
		jobConf.setReducerClass(WordCountReducer.class);

		// Specify input and output format
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		// Specify input and output directories
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		client.setConf(jobConf);
		
		try {
			JobClient.runJob(jobConf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
