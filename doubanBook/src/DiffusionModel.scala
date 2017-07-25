import org.apache.spark.graphx._

/**
  * Created by taoshu on 17-1-17.
  */
class DiffusionModel(myGraf: Graph[(String,Double,Double,String), Double]
                     , seed: Array[VertexId]) extends java.io.Serializable{
  /**
  this Class contains two diffusion models which can use seeds nodes
    to spread the information in a certain control like iteration.
   */

  private  var prob:Double=0.15
  private  var threshold:Int=15
  private  var numIter:Int=3
  private  var seeds: Array[VertexId]=seed

  def getFlagFromSeeds(vid: VertexId, seeds: Array[VertexId]): Boolean = {
    var flag: Boolean = false
      for (i <- seeds.indices if !flag )
        if (vid == seeds(i))
          flag = true
      flag
  }

  def resetSeeds(externalSeeds: Array[VertexId]): Unit ={
    this.seeds = externalSeeds
  }


  def setParameters(numIter:Int,prob:Double=0.15,threshold:Int=3): Unit ={
      this.prob = prob
      this.threshold = threshold
      this.numIter = numIter
  }
  
  def trustCalculator(v:Double):Double={
    val p = scala.util.Random.nextInt(1000) / 1000.0

    if (v>0.8)//2
      1
    else if (v<0.1)//0.15
      0
    else
      p*v
  }

  //ICModel
  def ICModel():Array[VertexId]={
    //0表示未激活，1表示激活未传播，2表示激活但已传播
    var result = seeds
    var j: Int = 0
    var influenced: VertexRDD[Double] = null
    var GWithSeeds = myGraf.mapVertices((_,attr)=>attr._3)
    GWithSeeds = myGraf.mapVertices{
      (id, attr) =>
        if (getFlagFromSeeds(id, seeds))
          1
        else
          0
    }
    while (j < numIter) {//控制扩散轮数

      influenced = GWithSeeds.aggregateMessages[Double](
        //设置种子节点的感染标志为true
        //为每次传播（即每条边）产生随机数与参数p
        //修改感染的节点属性
        triplet => {
          val trigger = trustCalculator(triplet.attr)
          if(triplet.srcAttr==1) {
            triplet.sendToSrc(2)
            if (trigger >= prob && triplet.dstAttr == 0) {
              triplet.sendToDst(1)
            } else {
              triplet.sendToDst(0)
            }
          }
        },
        (a, b) => {
          if (a==1||b==1)
            1
          else if (a==2||b==2)
            2
          else
            0
        }
      )
      j += 1
      result ++= influenced.filter(v => v._2!=0).map(v => v._1).distinct().collect()
      GWithSeeds = GWithSeeds.joinVertices(influenced)((_,_,attr)=>attr)
    }
    result.distinct
  }

  //LTModel
  def LTModel():Array[VertexId]={
//-2表示种子，-1表示已传播，大于等于0表示正在积累接受值
    var result = seeds
    var GWithSeeds = myGraf.mapVertices((_,attr)=>0.0)
    var j: Int = 0
    var influenced: VertexRDD[Double] = null
    GWithSeeds = myGraf.mapVertices{
      (id, _) =>
        if (getFlagFromSeeds(id, seeds))
          2
        else
          0
    }

    while(j<numIter){
      influenced= GWithSeeds.aggregateMessages[Double](
        triplet => {
          if(triplet.srcAttr!= -1) {
            if (triplet.srcAttr>= threshold || triplet.srcAttr == -2) {
              triplet.sendToDst(triplet.srcAttr)
              triplet.sendToSrc(-1)
            }
          }
        },
        (a,b)=>
          if(a== -1 || b== -1)
            -1
          else
            a+b
      )
      j+=1
      result ++= influenced.filter(v=>v._2 >= threshold ||v._2== -1).map(v=>v._1).distinct().collect()
      GWithSeeds = GWithSeeds.joinVertices(influenced)((_,_,attr)=>attr)
    }
    result.distinct
  }
}
