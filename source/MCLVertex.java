package com.diogenes.mcl;

import org.apache.giraph.edge.*;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.MathUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import java.util.*;
import java.io.*;
import com.diogenes.mcl.IdAndWeightWritable;


public class MCLVertex extends BasicComputation<IntWritable, NullWritable, FloatWritable, IdAndWeightWritable> {

	@Override
	public void compute(Vertex<IntWritable, NullWritable, FloatWritable> vertex,
		Iterable<IdAndWeightWritable> messages) throws IOException {
		if (getSuperstep() % 7 == 0) { 
			//Aggregate information about graph to check for terminate state
			if (getSuperstep() == 7) vertex.voteToHalt();
			
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
				ArrayList<Edge<IntWritable, FloatWritable> > edges = new ArrayList<Edge<IntWritable, FloatWritable> >(); // Array of updated edges
				for (Integer id: map.keySet()) { // Loop ids of target vertexes in edges
					edges.add(EdgeFactory.create(new IntWritable(id), new FloatWritable(map.get(id) * map.get(id) / sum_weight))); // Add new edge to array
					//edges.add(new DefaultEdge<IntWritable, FloatWritable>(new IntWritable(id), new FloatWritable(map.get(id) * map.get(id) / sum_weight))); // Add new edge to array
				}
				vertex.setEdges(edges); // Set edges


				// Send edges from start vertex of path
				for (Edge edge: vertex.getEdges()) { // Loop output edges
					sendMessage(vertex.getId(), new IdAndWeightWritable(vertex.getId().get(), ((FloatWritable)edge.getValue()).get())); // Send message to target vertexes of output edges
				}
			} else {
				//Send full path to start vertex of this path
				for (IdAndWeightWritable message: messages) { // Loop messages
					for (Edge edge: vertex.getEdges()) { // Loop output edges
						sendMessage(new IntWritable(message.getId()), new IdAndWeightWritable(((IntWritable)edge.getTargetVertexId()).get(), ((FloatWritable)edge.getValue()).get() * message.getWeight())); // Send weight of path to start vertex
					}
				}
			}
		}
	}
}