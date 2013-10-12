package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * An integer sort implementation.
 * 
 * @author Steven Huang
 * @version 1.0b
 * 
 */
public class IntegerSort {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf jobConf = new JobConf(IntegerSort.class);

		// Specify configuration files.
		jobConf.addResource(new Path("/opt/hadoop-1.1.2/conf/core-site.xml"));
		jobConf.addResource(new Path("/opt/hadoop-1.1.2/conf/hdfs-site.xml"));

		// Specify job name.
		jobConf.setJobName("Integer sort");

		// Specify output types
		jobConf.setOutputKeyClass(IntWritable.class);
		jobConf.setOutputValueClass(Text.class);

		// Specify mapper and reducer.
		jobConf.setMapperClass(IntegerSortMapper.class);
		jobConf.setReducerClass(IntegerSortReducer.class);

		// Specify input and output format.
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		// Specify input and output directories.
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		FileInputFormat.setInputPaths(jobConf, inputPath);
		FileOutputFormat.setOutputPath(jobConf, outputPath);

		// Remove existing output directory.
		try {
			FileSystem hdfs = outputPath.getFileSystem(jobConf);
			if (hdfs.exists(outputPath)) {
				hdfs.delete(outputPath, true);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		client.setConf(jobConf);

		try {
			JobClient.runJob(jobConf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
