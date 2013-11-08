package com.yullage.bigdata2013;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class PageRankDataReader
		extends
		VertexInputReader<LongWritable, Text, Text, NullWritable, DoubleWritable> {
	@Override
	public boolean parseVertex(LongWritable key, Text value,
			Vertex<Text, NullWritable, DoubleWritable> vertex) throws Exception {
		System.out.println(value.toString());
		System.out.println("==============================");

		String line = value.toString();
		String[] tokens = line.split("\\s+");
		int tokenCount = tokens.length;

		// Skip invalid lines and pages without any out link.
		if (tokenCount < 2) {
			return false;
		}

		vertex.setVertexID(new Text(tokens[0]));
		for (int i = 1; i < tokenCount; i++) {
			System.out.println(tokens[i]);
			vertex.addEdge(new Edge<Text, NullWritable>(new Text(tokens[i]),
					null));
		}

		return true;
	}
}
