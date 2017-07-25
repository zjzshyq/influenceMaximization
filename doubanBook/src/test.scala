import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer
/**
  * Created by hyq on 17-5-13.
  */
object test {
  def main(args:Array[String]):Unit= {
    //var tableBoundary: Array[(Int,Int,Boolean)] =null
    //    var tableBoundary=ArrayBuffer[(Int,Boolean)]()
    //    var seeds=ArrayBuffer[Int]()
    //    var i=0
    //    while(i<10) {
    //      tableBoundary = tableBoundary :+ (i, false)
    //      i+=1
    //    }
    //    tableBoundary = tableBoundary.sortBy(_._1).reverse
    //    tableBoundary.foreach{
    //      v=>
    //        println(v)
    //    }
//    val s ="a;b;c;d"
//    val s2 = "a;d;f;g;h"
//    var team=ArrayBuffer[String]()
//    var team2 :Array[String] =null
//    s.split(";").foreach{
//      t=>
//        team+=t
//    }
//    s2.split(";").foreach{
//      t=>
//        team+=t
//    }
//    team.foreach{
//      t=>
//        println(t)
//    }
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
//    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
//      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
//
//    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
//      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
//      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
//    val myGraph = Graph(myVertices, myEdges)
//    val tmp = myGraph.mapVertices((id,attr)=>1)
//    val tv=1
//    val a =tmp.inDegrees.foreach {
//      v=>
//      1
//    }
//    println(a)
    //vertices=234, edges=8420
    val path2 = "/snap/Circles/subEgo"
    val building2 = new BuildGraph(sc,path2)
    val graf2 = building2.buildFromEgonet()
    val graf = graf2.mapVertices((_,_)=>("1",0.0,0.0,"1"))
    println(graf.vertices.count)
//    val Algo = new IMA(graf)
//    Algo.setModel(0,3,0.15)
//    val tup = Algo.CELFpp(2)
//    tup._1.foreach(println)


//    val b =myGraph.mapVertices((id,attr)=>0).joinVertices(tmp.vertices) {
//      (_, _, a) =>a
//    }.mapVertices((id,attr)=>(attr,1))
//b.vertices.filter(v=>v._2._2==1).collect().foreach(println)
//val seeds = Array(1,3,4)
//    val i=0
//    for (i<-seeds.indices){
//       println(i)
//    }

//    tmp.vertices.collect().foreach(println)

  }
}
