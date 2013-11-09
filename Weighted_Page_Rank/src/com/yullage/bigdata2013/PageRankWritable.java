package com.yullage.bigdata2013;

import org.apache.hadoop.io.MapWritable;

public class PageRankWritable extends MapWritable {
	@Override
	public String toString() {
		return this.get(PageRankVertex.KEY_RANK).toString();
	}
}
