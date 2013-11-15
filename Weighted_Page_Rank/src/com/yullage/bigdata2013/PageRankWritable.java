package com.yullage.bigdata2013;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

/**
 * @author Yu-chun Huang
 * @version 1.0b
 */
public class PageRankWritable extends MapWritable {
	private static final Text KEY_RANK = new Text("__KEY_RANK");
	private static final Text KEY_SENDER_VERTEX_ID = new Text("__KEY_SENDER_VERTEX_ID");
	private static final Text KEY_IN_EDGE_COUNT = new Text("__KEY_IN_VERTEX_COUNT");
	private static final Text KEY_OUT_EDGE_COUNT = new Text("__KEY_OUT_VERTEX_COUNT");
	private static final Text KEY_WEIGHT = new Text("__KEY_WEIGHT");

	/**
	 * @return Rank
	 */
	public DoubleWritable getRank() {
		if (!this.containsKey(KEY_RANK)) {
			this.put(KEY_RANK, new DoubleWritable());
		}
		return (DoubleWritable) this.get(KEY_RANK);
	}

	/**
	 * @param rank
	 */
	public void setRank(DoubleWritable rank) {
		getRank().set(rank.get());
	}

	/**
	 * @param rank
	 */
	public void setRank(double rank) {
		getRank().set(rank);
	}

	/**
	 * @return Sender vertex ID
	 */
	public Text getSenderId() {
		if (!this.containsKey(KEY_SENDER_VERTEX_ID)) {
			this.put(KEY_SENDER_VERTEX_ID, new Text());
		}
		return (Text) this.get(KEY_SENDER_VERTEX_ID);
	}

	/**
	 * @param vertexId
	 */
	public void setSenderId(Text vertexId) {
		getSenderId().set(vertexId);
	}

	/**
	 * @return Incoming edge count
	 */
	public LongWritable getInEdgeCount() {
		if (!this.containsKey(KEY_IN_EDGE_COUNT)) {
			this.put(KEY_IN_EDGE_COUNT, new LongWritable());
		}
		return (LongWritable) this.get(KEY_IN_EDGE_COUNT);
	}

	/**
	 * @param inEdgeCount
	 */
	public void setInEdgeCount(LongWritable inEdgeCount) {
		getInEdgeCount().set(inEdgeCount.get());
	}

	/**
	 * @param inEdgeCount
	 */
	public void setInEdgeCount(long inEdgeCount) {
		getInEdgeCount().set(inEdgeCount);
	}

	/**
	 * @return Outgoing edge count
	 */
	public LongWritable getOutEdgeCount() {
		if (!this.containsKey(KEY_OUT_EDGE_COUNT)) {
			this.put(KEY_OUT_EDGE_COUNT, new LongWritable());
		}
		return (LongWritable) this.get(KEY_OUT_EDGE_COUNT);
	}

	/**
	 * @param outEdgeCount
	 */
	public void setOutEdgeCount(LongWritable outEdgeCount) {
		getOutEdgeCount().set(outEdgeCount.get());
	}

	/**
	 * @param outEdgeCount
	 */
	public void setOutEdgeCount(long outEdgeCount) {
		getOutEdgeCount().set(outEdgeCount);
	}

	/**
	 * @return Map of weight values for each neighbor
	 */
	public MapWritable getWeightMap() {
		if (!this.containsKey(KEY_WEIGHT)) {
			this.put(KEY_WEIGHT, new MapWritable());
		}
		return (MapWritable) this.get(KEY_WEIGHT);
	}

	/**
	 * @param weightMap
	 */
	public void setWeightMap(MapWritable weightMap) {
		getWeightMap().putAll(weightMap);
	}

	/**
	 * @param vertexId
	 * @return Weight values of the specified neighbor
	 */
	public DoubleWritable getWeight(Text vertexId) {
		if (getWeightMap().containsKey(vertexId)) {
			return (DoubleWritable) getWeightMap().get(vertexId);
		} else {
			return null;
		}
	}

	/**
	 * @param vertexId
	 * @param weight
	 */
	public void setWeight(Text vertexId, DoubleWritable weight) {
		getWeightMap().put(vertexId, weight);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getRank().toString();
	}
}
