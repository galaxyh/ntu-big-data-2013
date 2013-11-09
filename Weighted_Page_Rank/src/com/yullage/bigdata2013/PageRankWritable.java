package com.yullage.bigdata2013;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TextArrayWritable;

public class PageRankWritable extends MapWritable {
	private static final Text KEY_RANK = new Text("_KEY_RANK");
	private static final Text KEY_IN_VERTEX = new Text("_KEY_IN_VERTEX");
	private static final Text KEY_WEIGHT = new Text("_KEY_WEIGHT");

	public DoubleWritable getRank() {
		if (!this.containsKey(KEY_RANK)) {
			this.put(KEY_RANK, new DoubleWritable());
		}
		return (DoubleWritable) this.get(KEY_RANK);
	}

	public void setRank(DoubleWritable rank) {
		getRank().set(rank.get());
	}

	public void setRank(double rank) {
		getRank().set(rank);
	}

	public TextArrayWritable getInVertexArray() {
		if (!this.containsKey(KEY_IN_VERTEX)) {
			this.put(KEY_IN_VERTEX, new TextArrayWritable());
		}
		return (TextArrayWritable) this.get(KEY_IN_VERTEX);
	}

	public void setInVertexArray(TextArrayWritable inVertexArray) {
		getInVertexArray().set((Writable[]) inVertexArray.toArray());
	}

	public MapWritable getWeightMap() {
		if (!this.containsKey(KEY_WEIGHT)) {
			this.put(KEY_WEIGHT, new MapWritable());
		}
		return (MapWritable) this.get(KEY_WEIGHT);
	}

	public void setWeightMap(MapWritable weightMap) {
		getWeightMap().putAll(weightMap);
	}

	public DoubleWritable getWeight(Text vertexId) {
		if (getWeightMap().containsKey(vertexId)) {
			return (DoubleWritable) getWeightMap().get(vertexId);
		} else {
			return null;
		}
	}

	public void setWeight(Text vertexId, DoubleWritable weight) {
		getWeightMap().put(vertexId, weight);
	}

	@Override
	public String toString() {
		return getRank().toString();
	}
}
