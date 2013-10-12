package com.yullage.bigdata2013;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * A page rank implementation.
 * 
 * @author Steven Huang
 * @version 1.0b
 * 
 */
public class PageRank {
	public static final float DAMPING = 0.85F;
	public static final String OUT_LINK_FLAG = "$";
	private static final String TEMP_INPUT_PATH = "YullageTempInput";

	public static void main(String[] args) {
		FileSystem hdfs;
		JobClient client = new JobClient();
		JobConf jobConf = new JobConf(PageRank.class);

		// Specify configuration files.
		jobConf.addResource(new Path("/opt/hadoop-1.1.2/conf/core-site.xml"));
		jobConf.addResource(new Path("/opt/hadoop-1.1.2/conf/hdfs-site.xml"));

		// Specify job name.
		jobConf.setJobName("Page rank");

		// Specify output types.
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);

		// Specify mapper and reducer.
		jobConf.setMapperClass(PageRankMapper.class);
		jobConf.setReducerClass(PageRankReducer.class);

		// Specify input and output format.
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		// Specify input and output directories.
		Path inputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		Path tmpInputPath = new Path(TEMP_INPUT_PATH);

		int iterations = Integer.parseInt(args[0]);

		// Remove existing output directory.
		try {
			hdfs = outputPath.getFileSystem(jobConf);
			if (hdfs.exists(outputPath)) {
				hdfs.delete(outputPath, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// First iteration. Use original input path.
		System.out.println("Iteration 1...");
		FileInputFormat.setInputPaths(jobConf, inputPath);
		FileOutputFormat.setOutputPath(jobConf, outputPath);
		client.setConf(jobConf);

		try {
			JobClient.runJob(jobConf);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Second and later iterations. Use temporary input directory.
		for (int i = 1; i < iterations; i++) {
			System.out.println("Iteration " + (i + 1) + "...");
			try {
				hdfs = tmpInputPath.getFileSystem(jobConf);
				if (hdfs.exists(tmpInputPath)) {
					hdfs.delete(tmpInputPath, true);
				}

				hdfs = outputPath.getFileSystem(jobConf);
				hdfs.rename(outputPath, tmpInputPath);
			} catch (IOException e) {
				e.printStackTrace();
			}

			FileInputFormat.setInputPaths(jobConf, tmpInputPath);
			FileOutputFormat.setOutputPath(jobConf, outputPath);
			client.setConf(jobConf);

			try {
				JobClient.runJob(jobConf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// Clean up.
		try {
			hdfs = tmpInputPath.getFileSystem(jobConf);
			if (hdfs.exists(tmpInputPath)) {
				hdfs.delete(tmpInputPath, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
