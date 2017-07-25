import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by taoshu on 17-1-24.
  */
class IMA (g:Graph[(String,Double,Double,String), Double]) extends java.io.Serializable{
  /**
  this Class contains several Influence Maximization Algorithm(IMA)s
    which are based on the diffusion models to find the seeds, to spread the
    influence as broad as they can.
    */
  private var model:Int=0
  private var numIter:Int = 0
  private var parameter:Double = 0

  def setModel(model:Int,numIter:Int,prob:Double=0.15,threshold:Int=3): Unit = {
    //IMAlgorithms are all based on diffusion model,
    // therefor we should set the diffusion model before implement IMA
    this.model = model
    this.numIter = numIter

    if (this.model == 0) {
      parameter = prob
    }
    else {
      parameter = parameter.toInt
      parameter = threshold
    }
  }


  def naiveGreedy(k:Int):Tuple2[Array[VertexId],Int] ={
    var i:Int = 0
    var seeds=ArrayBuffer[VertexId]()
    var numInfluenced:Int = 0
    g.cache()

    if (model!=0) {//扩散模型判断
      println("Can Only Apply ICModel for now!")
      (seeds.toArray,0)
    }

    else {//start to do some greed
      val Model = new DiffusionModel(g,seeds.toArray)
      var inode :VertexId = 0
      while (i < k) {
        var max =0
        var tmp = seeds
        g.vertices.collect.foreach {
          v =>
            tmp+=v._1
            println(g.edges.count())
            Model.resetSeeds(tmp.toArray)
            Model.setParameters(numIter,parameter)
            val num:Int = Model.ICModel().length
            if(num>max){
              max = num
              inode = v._1
            }
            tmp.trimEnd(1)//移除结尾处的节点
        }
        i += 1
        seeds+=inode
        numInfluenced = max
      }
      (seeds.toArray,numInfluenced)
    }
  }

  def TJT(kBar:Int,hBar:Double):Tuple2[Array[VertexId],Int]={
    var i:Int = 0
    var seeds=ArrayBuffer[VertexId]()
    var numInfluenced:Int = 0

    val h =(kBar*hBar).toInt
    val k = kBar-h
    g.cache()
    g.vertices.sortBy(_._2._2,ascending = false).take(h).foreach{
      v=>
        seeds+=v._1
    }

    if (model!=0) {//扩散模型判断
      println("Can Only Apply ICModel for now!")
      (seeds.toArray,0)
    }else if( hBar>1 || hBar<0){
      println("range of h should in (0,1)")
      (seeds.toArray,0)
    }else if(kBar <0){
      println("k should over 0")
      (seeds.toArray,0)
    }
    else {//start to do some greed
    val Model = new DiffusionModel(g,seeds.toArray)
      var inode :VertexId = 0
      while (i < k) {
        var max =0
        var tmp = seeds
        g.vertices.collect.foreach {
          v =>
            tmp+=v._1
            Model.resetSeeds(tmp.toArray)
            Model.setParameters(numIter,parameter)
            val num:Int = Model.ICModel().length
            if(num>max){
              max = num
              inode = v._1
            }
            tmp.trimEnd(1)//移除结尾处的节点
        }
        i += 1
        seeds+=inode
        numInfluenced = max
      }
      (seeds.toArray,numInfluenced)
    }

  }

  def TIMM(kBar:Int,hBar:Double):Tuple2[Array[VertexId],Int]={
  //针对CLEFpp部分增加了小组的判断,需要v._4
    var i:Int = 0
    var seeds=ArrayBuffer[VertexId]()
    var team=ArrayBuffer[String]()
    var numInfluenced:Int = 0
    var tableBoundary=ArrayBuffer[(VertexId,Int,Boolean,String)]()
    //var tableBoundary: Array[(VertexId,Int,Boolean)] =null

    val h =(kBar*hBar).toInt
    val k = kBar-h
    g.cache()
    g.vertices.sortBy(_._2._2,ascending = false).take(h).foreach{
      v=>
        seeds+=v._1
        v._2._4.split(";").foreach{
          t=>
            team+=t
        }
    }
    team = team.distinct

    if (model!=0) {//扩散模型判断
      println("Can Only Apply ICModel for now!")
      (seeds.toArray,numInfluenced)
    }else if( hBar>1 || hBar<0){
      println("range of h should in (0,1)")
      (seeds.toArray,0)
    }else if(kBar <0){
      println("k should over 0")
      (seeds.toArray,0)
    }
    else {
      //start to run CELFpp
      val Model = new DiffusionModel(g,seeds.toArray)
      //      val tmp=seeds
      var inode :VertexId = 0
      var i=0
      //first round to make a heap
      val nodes = g.vertices.collect()
      nodes.foreach{
        v=>
          i+=1
          //就是每个tmp代表一个节点
          var tmp=seeds
          tmp+=v._1
          Model.resetSeeds(tmp.toArray)
          Model.setParameters(numIter,parameter)
          //在Heap中追加元素
          tableBoundary =tableBoundary:+ (v._1,Model.ICModel().length,false,v._2._4)
      }
  //    tableBoundary = tableBoundary.sortBy(_._2).reverse
  //    seeds+=tableBoundary(0)._1
  //    tableBoundary(0) = (tableBoundary(0)._1,tableBoundary(0)._2,true,tableBoundary(0)._4)

      //left round
      breakable{
        nodes.foreach{
          v=>
            //每个tmp代表一个临时种子集合
            var tmp=seeds
            var team1 = ArrayBuffer[String]()
            var team2 = team
            //取一个与seed值不同的节点
            //另外该节点不能seed处于同一小组
            breakable {
              seeds.foreach {
                s =>
                  if(v._1!=s)
                    break()
              }
              //小组判断
              v._2._4.split(";").foreach{
                t=>
                  team1+=t
                  team2+=t
                  team2 = team2.distinct
                  team1 = team1.distinct
              }
              if (team1.length+team.length!=team2.length) {
                team = team2
                break()
              }
            }
            tmp+=v._1

            Model.resetSeeds(tmp.toArray)
            Model.setParameters(numIter,parameter)
            numInfluenced = Model.ICModel().length
            var i=0
            var flag = false
            breakable{
              tableBoundary.foreach{
                v=>
                  if (!v._3 && numInfluenced>=v._2){
                    seeds+=v._1
                    tableBoundary(i) = (tableBoundary(i)._1,tableBoundary(i)._2,true,tableBoundary(i)._4)
                    i+=1
                    flag = true
                    break()
                  }
              }
            }
            if (!flag)
              tmp.trimEnd(1)//移除结尾处的节点
            if (seeds.length==k)
              return (seeds.toArray,numInfluenced)
        }
      }
      (seeds.toArray,numInfluenced)
    }
  }


  def CELFpp(k:Int):Tuple2[Array[VertexId],Int] ={
    //the full name of CELF is Cost-Effective Lazy Forward selection ++
    var i:Int = 0
    var seeds=ArrayBuffer[VertexId]()
    var numInfluenced:Int = 0
    var tableBoundary=ArrayBuffer[(VertexId,Int,Boolean)]()
    //var tableBoundary: Array[(VertexId,Int,Boolean)] =null

    g.cache()

    if (model!=0) {//扩散模型判断
      println("Can Only Apply ICModel for now!")
      (seeds.toArray,numInfluenced)
    }
    else {
      //start to run CELF
      val Model = new DiffusionModel(g,seeds.toArray)
//      val tmp=seeds
      var inode :VertexId = 0
      var i=0
      //first round to make a boundary table
      val nodes = g.vertices.collect()
      nodes.foreach{
        v=>
          i+=1
          //就是每个tmp代表一个节点
          var tmp=seeds
          tmp+=v._1
          Model.resetSeeds(tmp.toArray)
          Model.setParameters(numIter,parameter)
          //在Heap中追加元素
          tableBoundary =tableBoundary:+ (v._1,Model.ICModel().length,false)
      }
      tableBoundary = tableBoundary.sortBy(_._2).reverse
      seeds+=tableBoundary(0)._1
      tableBoundary(0) = (tableBoundary(0)._1,tableBoundary(0)._2,true)

      //left round
      breakable{
        nodes.foreach{
          v=>
            //每个tmp代表一个节点
            var tmp=seeds

            //取一个与seed值不同的节点
            breakable {
              seeds.foreach {
                s =>
                  if(v._1!=s)
                    break()
              }
            }
            tmp+=v._1

            Model.resetSeeds(tmp.toArray)
            Model.setParameters(numIter,parameter)
            numInfluenced = Model.ICModel().length
            var i=0
            var flag = false
            breakable{
              tableBoundary.foreach{
                v=>
                  if (!v._3 && numInfluenced>=v._2){
                    seeds+=v._1
                    tableBoundary(i) = (tableBoundary(i)._1,tableBoundary(i)._2,true)
                    i+=1
                    flag = true
                    break()
                  }
              }
            }
            if (!flag)
              tmp.trimEnd(1)//移除结尾处的节点
            if (seeds.length==k)
              return (seeds.toArray,numInfluenced)
        }
      }
      (seeds.toArray,numInfluenced)
    }
  }

  def getNeiberSeedNum(node:VertexId,seed:ArrayBuffer[VertexId]):Int={
    var num:Int =0
    //error:java.lang.NullPointerException
    if (g.edges==null)
      0
    else if(seed.length==0)
      0
    else {
      val e2 = g.edges.filter(v => v.srcId == node)
      if (e2==null)
        0
      else {
        e2.collect.foreach {
          e =>
            var i = 0
            while (i < seed.length) {
              if (e.dstId == seed(i))
                num += 1
              i+=1
            }
        }
      }
      num
    }
    num
  }

  def notinSeeds(v:VertexId,seeds:ArrayBuffer[VertexId]):Boolean={
    var i=0
    var flag = true
    while(i<seeds.length){
      if (seeds(i)==v) {
        flag = false
      }
      i+=1
    }
    flag
  }

  def DegreeDiscount(k:Int):Array[VertexId] ={
    var i:Int = 0
    var seeds=ArrayBuffer[VertexId]()
    var tmpSeed:VertexId= -1
    g.cache()
    while(i<k){
      val g2 = g
      g2.outDegrees.collect().foreach{
        var max = 0.0
        var tv = 0
        v=>
          if(seeds.length>=1) {
            tv = getNeiberSeedNum(v._1, seeds)
          }
          val dd = v._2-2*tv - (v._2-tv)*tv*this.parameter
          if ( dd>max && notinSeeds(v._1,seeds)) {
            max = dd
            tmpSeed = v._1
          }
      }
      seeds+=tmpSeed
      i+=1
    }
    seeds.toArray
  }

}
