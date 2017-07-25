import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by taoshu on 17-1-19.
  */
class BuildGraph(sc:SparkContext, textPath:String) {

  def buildFromEgonet():Graph[Int,Double]={

    val f: RDD[String] = this.sc.textFile(this.textPath)

//    //构建点
//    val myVertices: RDD[(VertexId, (Double,Boolean))] = f.map {
//      line =>
//        val feilds = line.split(":")
//        (feilds(0).toLong, (1.0,false))
//    }

    //构建边
    val tmpEdges = f.map {
      line =>
        val v = line.split(":")
        val node = v(0)
        var p = scala.util.Random.nextInt(1000) / 1000.0
        v(1).split(" ").map(st => (node, st, p)).filter(v => v._2 != "")
    }.flatMap(x => x)
    val myEdges: RDD[Edge[Double]] = tmpEdges.map {
      a =>
        Edge(a._1.toLong, a._2.toLong, a._3.toDouble)
    }.cache()

    val verticesSrc:RDD[(VertexId,Int)] = myEdges.map(a=>(a.srcId,1)).reduceByKey((a,b)=>a+b)
    val verticesDst:RDD[(VertexId,Int)] = myEdges.map(a=>(a.dstId,1)).reduceByKey((a,b)=>a+b)
    val vertices:RDD[(VertexId,Int)] = (verticesSrc++verticesDst).reduceByKey((a,b)=>1)
    Graph(vertices, myEdges)
  }


  def buildFromTSVandWeight():Graph[Int,Double]= {
    val f:RDD[String] = this.sc.textFile(this.textPath)
    val edges:RDD[Edge[Double]]=f.map(
      line=>{
        val fields = line.split("\t")
        Edge(fields(0).toLong,fields(1).toLong,fields(2).toDouble)
      }).cache()
    val verticesSrc:RDD[(VertexId,Int)] = edges.map(a=>(a.srcId,1)).reduceByKey((a,b)=>a+b)
    val verticesDst:RDD[(VertexId,Int)] = edges.map(a=>(a.dstId,1)).reduceByKey((a,b)=>a+b)
    val vertices:RDD[(VertexId,Int)] = (verticesSrc++verticesDst).reduceByKey((a,b)=>a+b)
    Graph(vertices,edges)
  }

  def buildFromCSVandWeight():Graph[Int,Double]= {
    val f:RDD[String] = this.sc.textFile(this.textPath)
    val edges:RDD[Edge[Double]]=f.map(
      line=>{
        val fields = line.split(",")
        Edge(fields(0).toLong,fields(1).toLong,fields(2).toDouble)
      }).cache()
    val verticesSrc:RDD[(VertexId,Int)] = edges.map(a=>(a.srcId,1)).reduceByKey((a,b)=>a+b)
    val verticesDst:RDD[(VertexId,Int)] = edges.map(a=>(a.dstId,1)).reduceByKey((a,b)=>a+b)
    val vertices:RDD[(VertexId,Int)] = (verticesSrc++verticesDst).reduceByKey((a,b)=>1)
    Graph(vertices,edges)
  }

  def buildFromDoubanCSV():Graph[(String,Double,Double,String),Double]= {
    val user:RDD[String] = sc.textFile("/snap/douban/doubanTrustNetwork/influenceUsers2.csv")
    val edge:RDD[String] = sc.textFile("/snap/douban/doubanTrustNetwork/trustEdges2.csv")
    val vertices:RDD[(VertexId,(String,Double,Double,String))] = user.map(
      line=>{
        val c = line.split(",")
        (c(0).toLong,(c(1),c(2).toDouble,0,c(3)))
      }
    )
    val edges:RDD[Edge[Double]]=edge.map(
      line=>{
        val fields = line.split(",")
        Edge(fields(0).toLong, fields(1).toLong, fields(2).toDouble)
      })

    Graph(vertices,edges)
  }

  def buildFromDoubanCSV2(g:Graph[Int,Double]):Graph[(Double,Boolean),Double]={
    g.mapVertices((id,_)=>(id.toDouble,false))
  }

  def buildFromTSV():Graph[Int,Int]= {
    GraphLoader.edgeListFile(sc,textPath)
  }
}
