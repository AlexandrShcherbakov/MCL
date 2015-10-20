package com.lesandr.mcl;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MathUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import java.util.*;
import java.io.*;


public class MCLVertex extends Vertex<IntWritable, NullWritable, FloatWritable, IdAndWeightWritable> {

	private class IdAndWeightWritable implements WritableComparable {
		private int id;
		private float weight;

		public IdAndWeightWritable() {
		}

		public IdAndWeightWritable(int id, float weight) {
			this.id = id;
			this.weight = weight;
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(id);
			out.writeFloat(weight);
		}

		public void readFields(DataInput in) throws IOException {
			id = in.readInt();
			weight = in.readFloat();
		}

		public int compareTo(IdAndWeightWritable o) {
			return id < o.id ? -1 : (id == o.id ? 0 : 1);
		}

		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + id;
			result = prime * result + (int)(1 / weight);
			return result;
		}

		public int getId() {
			return id;
		}

		public float getWeight() {
			return weight;
		}

		public void setId(int id) {
			this.id = id;
		}

		public void setWeight(float weight) {
			this.weight = weight;
		}

		public void set(int id, float weight) {
			this.id = id;
			this.weight = weight;
		}

		public String toString() {
			return Integer.toString(id) + ":" + Float.toString(weight);
		}
	}




	@Override
	public void compute(Iterable<IdAndWeightWritable> messages) throws IOException {
		if (getSuperstep() % 7 == 0) { 
			//Aggregate information about graph to check for terminate state
			
		} else {
			if (getSuperstep() % 7 % 2 == 1) {
				// Use messages for edges changing

				// Aggregate weightes for each target vertex
				Map<Integer, Float> map = new HashMap<Integer, Float>(); // Structure for aggregation of weight from each target vertex
				for (IdAndWeightWritable message : messages) { // Loop messages
					if (!map.containsKey(message.getId())) { // Check if Id of vertex absent in aggregator
						map.put(message.getId(), 0.0f); // Initialize of new Id of vertex
					}
					map.put(message.getId(), map.get(message.getId()) + message.getWeight()); // Add weight of path in aggregator		
				}

				// Compute sum of square of weights of output edges
				float sum_weight = 0; // Initialization of sum
				for (float weight: map.values()) { // Loop weights of output edges
					sum_weight += weight * weight; // Add square of weight
				}

				// Set new output edges for this vertex
				ArrayList<DefaultEdge<Integer, Float> > edges = new ArrayList<DefaultEdge<Integer, Float> >(); // Array of updated edges
				for (Integer id: map.keySet()) { // Loop ids of target vertexes in edges
					edges.add(new DefaultEdge<Integer, Float>(id, map.get(id) * map.get(id) / sum_weight)); // Add new edge to array
				}
				setEdges(edges); // Set edges


				// Send edges from start vertex of path
				for (DefaultEdge edge: getEdges()) { // Loop output edges
					sendMessage(getId(), new IdAndWeightWritable(getId(), edge.getValue())); // Send message to target vertexes of output edges
				}
			} else {
				//Send full path to start vertex of this path
				for (IdAndWeightWritable message: messages) { // Loop messages
					for (DefaultEdge edge: getEdges()) { // Loop output edges
						sendMessage(message.getId(), new IdAndWeightWritable(edge.getTargetVertexId(), edge.getValue() * message.getWeight())); // Send weight of path to start vertex
					}
				}
			}
		}
	}
}