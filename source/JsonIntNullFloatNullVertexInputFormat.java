package com.diogenes.mcl;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.apache.giraph.io.formats.*;


import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features <code>int</code> vertex ID's,
  * <code>Null</code> vertex values and <code>float</code>
  * out-edge weights, and <code>Null</code> message types,
  *  specified in JSON format.
  *
  * =)
  */
public class JsonIntNullFloatNullVertexInputFormat extends
  TextVertexInputFormat<IntWritable, NullWritable, FloatWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new JsonIntNullFloatNullVertexReader();
  }

 /**
  * VertexReader that features <code>double</code> vertex
  * values and <code>float</code> out-edge weights. The
  * files should be in the following JSON format:
  * JSONArray(<vertex id>, <vertex value>,
  *   JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
  * Here is an example with vertex id 1, vertex value 4.3, and two edges.
  * First edge has a destination vertex 2, edge value 2.1.
  * Second edge has a destination vertex 3, edge value 0.7.
  * [1,4.3,[[2,2.1],[3,0.7]]]
  */
  class JsonIntNullFloatNullVertexReader extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
    JSONException> {

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected IntWritable getId(JSONArray jsonVertex) throws JSONException,
              IOException {
      return new IntWritable(jsonVertex.getInt(0));
    }

    @Override
    protected NullWritable getValue(JSONArray jsonVertex) throws
      JSONException, IOException {
      return NullWritable.get();//jsonVertex.getDouble(1));
    }

    @Override
    protected Iterable<Edge<IntWritable, FloatWritable>> getEdges(
        JSONArray jsonVertex) throws JSONException, IOException {
      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
      List<Edge<IntWritable, FloatWritable>> edges =
          Lists.newArrayListWithCapacity(jsonEdgeArray.length());
      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
        edges.add(EdgeFactory.create(new IntWritable(jsonEdge.getInt(0)),
            new FloatWritable((float) jsonEdge.getDouble(1))));
      }
      return edges;
    }

    @Override
    protected Vertex<IntWritable, NullWritable, FloatWritable>
    handleException(Text line, JSONArray jsonVertex, JSONException e) {
      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }

  }
}