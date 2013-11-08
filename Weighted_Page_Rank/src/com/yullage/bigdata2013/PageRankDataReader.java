package com.yullage.bigdata2013;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class PageRankDataReader
		extends
		VertexInputReader<Text, TextArrayWritable, Text, NullWritable, DoubleWritable> {
	@Override
	public boolean parseVertex(Text key, TextArrayWritable value,
			Vertex<Text, NullWritable, DoubleWritable> vertex) throws Exception {
		vertex.setVertexID(key);

		System.out.println(value);
		for (Writable v : value.get()) {
			vertex.addEdge(new Edge<Text, NullWritable>((Text) v, null));
		}

		return true;
	}
}
