/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import breeze.linalg.{Vector => BV}

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
 * PageRank algorithm implementation. There are two implementations of PageRank implemented.
 *
 * The first implementation uses the standalone `Graph` interface and runs PageRank
 * for a fixed number of iterations:
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 1.0 )
 * for( iter <- 0 until numIter ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n ) {
 *     PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * The second implementation uses the `Pregel` interface and runs PageRank until
 * convergence:
 *
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 0.0 )
 * while( max(abs(PR - oldPr)) > tol ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n if abs(PR[i] - oldPR[i]) > tol ) {
 *     PR[i] = alpha + (1 - \alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of
 * neighbors which link to `i` and `outDeg[j]` is the out degree of vertex `j`.
 *
 * @note This is not the "normalized" PageRank and as a consequence pages that have no
 * inlinks will have a PageRank of alpha.
 */
object PageRank extends Logging {


  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int,
    resetProb: Double = 0.15): Graph[Double, Double] =
  {
    runWithOptions(graph, numIter, resetProb)
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   */
  def runWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15,
      srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    val numVertices = graph.numVertices
    val degGraph: Graph[Int, ED] = graph
      // Associate the out degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }

    // Sinks are vertices without outgoing edges, we need special handling for them.
    val sinks: VertexRDD[Int] = degGraph.vertices.filter(_._2 == 0).cache()
    val hasSink = sinks.count() > 0

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    // When running personalized pagerank, only the source vertex
    // has an attribute 1.0. All others are set to 0.
    var rankGraph: Graph[Double, Double] = degGraph
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) =>
        if (!(id != src && personalized)) 1.0 else 0.0
      }

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // If the graph has sinks compute the rank that would otherwise be distributed to their
      // outgoing edges so that we can redistribute it to the graph uniformly or to the source
      // vertex in the personalized case.
      val rankFromSinks: Double = if (hasSink) {
        rankGraph.outerJoinVertices(sinks) {
          (id, rank, zeroOpt) => if (zeroOpt.isDefined) rank else 0.0
        }.vertices.values.sum() * (1.0 - resetProb)
      } else {
        0.0
      }

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      val rPrb = if (personalized) {
        (src: VertexId, id: VertexId) => (resetProb + rankFromSinks) * delta(src, id)
      } else {
        (src: VertexId, id: VertexId) => resetProb + rankFromSinks / numVertices
      }

      // Using outer join so that all vertices are updated.
      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (id, oldRank, msgSumOpt) => rPrb(src, id) + (1.0 - resetProb) * msgSumOpt.getOrElse(0.0)
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logInfo(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }
    sinks.unpersist(false)

    rankGraph
  }

  /**
   * Run Personalized PageRank for a fixed number of iterations, for a
   * set of starting nodes in parallel. Returns a graph with vertex attributes
   * containing the pagerank relative to all starting nodes (as a sparse vector) and
   * edge attributes the normalized edge weight
   *
   * @tparam VD The original vertex attribute (not used)
   * @tparam ED The original edge attribute (not used)
   *
   * @param graph The graph on which to compute personalized pagerank
   * @param numIter The number of iterations to run
   * @param resetProb The random reset probability
   * @param sources The list of sources to compute personalized pagerank from
   * @return the graph with vertex attributes
   *         containing the pagerank relative to all starting nodes (as a sparse vector
   *         indexed by the position of nodes in the sources list) and
   *         edge attributes the normalized edge weight
   */
  def runParallelPersonalizedPageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
    numIter: Int, resetProb: Double = 0.15,
    sources: Array[VertexId]): Graph[Vector, Double] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    require(sources.nonEmpty, s"The list of sources must be non-empty," +
      s" but got ${sources.mkString("[", ",", "]")}")

    // TODO if one sources vertex id is outside of the int range
    // we won't be able to store its activations in a sparse vector
    require(sources.max <= Int.MaxValue.toLong,
      s"This implementation currently only works for source vertex ids at most ${Int.MaxValue}")

    val numVertices = graph.numVertices
    val degGraph: Graph[Int, ED] = graph
      // Associate the out degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }

    // Sinks are vertices without outgoing edges, we need special handling for them.
    val sinks: VertexRDD[Int] = degGraph.vertices.filter(_._2 == 0).cache()
    val hasSink = sinks.count() > 0

    val zero = Vectors.sparse(sources.size, List()).asBreeze
    val sourcesInitMap = sources.zipWithIndex.map { case (vid, i) =>
      val v = Vectors.sparse(sources.size, Array(i), Array(1.0)).asBreeze
      (vid, v)
    }.toMap
    val sc = graph.vertices.sparkContext
    val sourcesInitMapBC = sc.broadcast(sourcesInitMap)
    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each source vertex with attribute 1.0.
    var rankGraph = degGraph
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices { (vid, attr) =>
        if (sourcesInitMapBC.value contains vid) {
          sourcesInitMapBC.value(vid)
        } else {
          zero
        }
      }

    var i = 0
    while (i < numIter) {
      val prevRankGraph = rankGraph
      // Propagates the message along outbound edges
      // and adding start nodes back in with activation resetProb
      val rankUpdates = rankGraph.aggregateMessages[BV[Double]](
        ctx => ctx.sendToDst(ctx.srcAttr :* ctx.attr),
        (a : BV[Double], b : BV[Double]) => a :+ b, TripletFields.Src)

      // If the graph has sinks compute the rank that would otherwise be distributed to their
      // outgoing edges so that we can redistribute it to the graph uniformly or to the source
      // vertex in the personalized case.
      val rankFromSinks: BV[Double] = if (hasSink) {
        rankGraph.outerJoinVertices(sinks) {
          (id, rank, zeroOpt) => if (zeroOpt.isDefined) rank else zero
        }.vertices.values.fold(zero)(_ :+ _) :* (1.0 - resetProb)
      } else {
        zero
      }
      val rankFromSinksBC = sc.broadcast(rankFromSinks)

      // Using outer join so that all vertices are updated.
      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (vid, oldRank, msgSumOpt) =>
          val popActivations: BV[Double] = msgSumOpt.getOrElse(zero) :* (1.0 - resetProb)
          val resetActivations = if (sourcesInitMapBC.value contains vid) {
            sourcesInitMapBC.value(vid) :* (rankFromSinksBC.value :+ resetProb)
          } else {
            zero
          }
          popActivations :+ resetActivations
        }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      logInfo(s"Parallel Personalized PageRank finished iteration $i.")

      i += 1
    }

    rankGraph.mapVertices { (vid, attr) =>
      Vectors.fromBreeze(attr)
    }
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
      runUntilConvergenceWithOptions(graph, tol, resetProb)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15,
      srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    val numVertices = graph.numVertices
    val degGraph: Graph[Int, ED] = graph
      // Associate the out degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }

    // Sinks are vertices without outgoing edges, we need special handling for them.
    val sinks: VertexRDD[Int] = degGraph.vertices.filter(_._2 == 0).cache()
    val hasSink = sinks.count() > 0

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    // When running personalized pagerank, only the source vertex
    // has an attribute 1.0. All others are set to 0.
    var rankGraph: Graph[(Double, Double), Double] = degGraph
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) =>
      if (!(id != src && personalized)) (1.0, Double.PositiveInfinity) else (0.0, 0.0)
    }

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var aboveTolCount = 1L
    var iteration = 0
    var prevRankGraph: Graph[(Double, Double), Double] = null
    while (aboveTolCount > 0) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._1 * ctx.attr), _ + _, TripletFields.Src)

      // If the graph has sinks compute the rank that would otherwise be distributed to their
      // outgoing edges so that we can redistribute it to the graph uniformly or to the source
      // vertex in the personalized case.
      val rankFromSinks: Double = if (hasSink) {
        rankGraph.outerJoinVertices(sinks) {
          (id, rankAndDelta, zeroOpt) => if (zeroOpt.isDefined) rankAndDelta._1 else 0.0
        }.vertices.values.sum() * (1.0 - resetProb)
      } else {
        0.0
      }

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      val rPrb = if (personalized) {
        (src: VertexId, id: VertexId) => (resetProb + rankFromSinks) * delta(src, id)
      } else {
        (src: VertexId, id: VertexId) => resetProb + rankFromSinks / numVertices
      }

      // Using outer join so that all vertices are updated.
      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (id, oldRankAndDelta, msgSumOpt) => {
          val rank = rPrb(src, id) + (1.0 - resetProb) * msgSumOpt.getOrElse(0.0)
          (rank, math.abs(rank - oldRankAndDelta._1))
        }
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      aboveTolCount = rankGraph.vertices.filter(vd => vd._2._2 > tol).count()
      logInfo(s"PageRank finished iteration $iteration, there is $aboveTolCount vertices with"
        + " delta greater than tol=$tol.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }
    sinks.unpersist(false)

    rankGraph.mapVertices((vid, attr) => attr._1)
  }

}
