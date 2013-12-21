package com.yullage.bigdata2013;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author Yu-chun Huang
 * @version 1.0b
 */
public class PageRankWritable implements Writable {
	private DoubleWritable rank = new DoubleWritable();
	private Text senderVertexId = new Text();
	private LongWritable inEdgeCount = new LongWritable();
	private LongWritable outEdgeCount = new LongWritable();
	private MapWritable weightMap = new MapWritable();

	/**
	 * @return Rank
	 */
	public DoubleWritable getRank() {
		return this.rank;
	}

	/**
	 * @param rank
	 */
	public void setRank(DoubleWritable rank) {
		this.rank = rank;
	}

	/**
	 * @param rank
	 */
	public void setRank(double rank) {
		this.rank.set(rank);
	}

	/**
	 * @return Sender vertex ID
	 */
	public Text getSenderId() {
		return this.senderVertexId;
	}

	/**
	 * @param vertexId
	 */
	public void setSenderId(Text vertexId) {
		this.senderVertexId = vertexId;
	}

	/**
	 * @return Incoming edge count
	 */
	public LongWritable getInEdgeCount() {
		return this.inEdgeCount;
	}

	/**
	 * @param inEdgeCount
	 */
	public void setInEdgeCount(LongWritable inEdgeCount) {
		this.inEdgeCount = inEdgeCount;
	}

	/**
	 * @param inEdgeCount
	 */
	public void setInEdgeCount(long inEdgeCount) {
		this.inEdgeCount.set(inEdgeCount);
	}

	/**
	 * @return Outgoing edge count
	 */
	public LongWritable getOutEdgeCount() {
		return this.outEdgeCount;
	}

	/**
	 * @param outEdgeCount
	 */
	public void setOutEdgeCount(LongWritable outEdgeCount) {
		this.outEdgeCount = outEdgeCount;
	}

	/**
	 * @param outEdgeCount
	 */
	public void setOutEdgeCount(long outEdgeCount) {
		this.outEdgeCount.set(outEdgeCount);
	}

	/**
	 * @return Map of weight values for each neighbor
	 */
	public MapWritable getWeightMap() {
		return this.weightMap;
	}

	/**
	 * @param weightMap
	 */
	public void setWeightMap(MapWritable weightMap) {
		this.weightMap = weightMap;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.rank.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		rank.readFields(in);
		senderVertexId.readFields(in);
		inEdgeCount.readFields(in);
		outEdgeCount.readFields(in);
		weightMap.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		rank.write(out);
		senderVertexId.write(out);
		inEdgeCount.write(out);
		outEdgeCount.write(out);
		weightMap.write(out);
	}
}
