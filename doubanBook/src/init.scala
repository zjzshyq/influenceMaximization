/**
  * Created by hyq on 17-4-2.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer
object init {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("douban").setMaster("local")
    val sc = new SparkContext(conf)


    //vertices=32813, edges=70W
    val path = "/snap/douban/edges2.csv"
    val building = new BuildGraph(sc,path)
    val graf = building.buildFromDoubanCSV()


    while(true) {
      print("Please enter type. 1 is diffuse, 2 is MIAlgorithm, 3 is exit: ")
      val chooseType = Console.readInt()
      var seed:Int = -1
      if (chooseType == 1) {
        print("Please enter model .1 is IC,2 is LT: ")
        val chooseModel = Console.readInt()
        val seeds:Array[VertexId] = Array(23949)
        val DModel = new DiffusionModel(graf,seeds)
        val tmpS = ArrayBuffer[(VertexId)]()
        print("Please enter the number of iterate(0-1000): ")
        val iter = Console.readInt()

        print("Please enter the seed(0-32812) till you type in -1: ")
        do{
          seed = Console.readInt()
          if(seed>32812 ||seed< -1) {
            println("wrong seed number")
            sys.exit(-1)
          }
          if(seed!= -1){
            tmpS+=seed
            print("enter another seed if u want, enter -1 to stop: ")
          }
        }while(seed!= -1)

        if (chooseModel==1){
          print("Please enter the diffusion probability(0-1): ")
          val p = Console.readDouble()
          if(p<0 || p>1){
            println("wrong input")
            sys.exit(-1)
          }
          DModel.setParameters(iter,p)
          DModel.resetSeeds(tmpS.toArray)
          val result = DModel.ICModel()
          println("the Influence of seeds is: "+result.length)
        }else if(chooseModel==2){
          print("Please enter the diffusion threshold(>0): ")
          val thres = Console.readDouble()
          if(thres<0){
            println("wrong input")
            sys.exit(-1)
          }
          DModel.setParameters(iter,thres)
          DModel.resetSeeds(seeds)
          val result = DModel.LTModel()
          println("the Influence of seeds is: "+result.length)
        }else{
          println("Please enter right Number, try again!")
        }
      } else if (chooseType == 2) {
        print("Please enter algorithm .1 is naiveGreedy, 2 is CELFpp, 3 is TJT, 4 is TIMM , 5 is DegreeDiscount: ")
        val chooseAglo = Console.readInt()
        val Algo = new IMA(graf)
        print("Please enter the number of seeds K(1-1000): ")
        val k = Console.readInt()
        print("Please enter the number of iterate(1-1000): ")
        val iter = Console.readInt()
        print("Please enter the diffusion probability(0-1): ")
        val p = Console.readDouble()
        if(p<0 || p>1){
          println("wrong input")
          sys.exit(-1)
        }
        if (chooseAglo==1){
          Algo.setModel(0,iter,p)
          val tup = Algo.naiveGreedy(k)
          tup._1.foreach(println)
          println(tup._2)
        }else if (chooseAglo==2){
          Algo.setModel(0,iter,p)
          val tup = Algo.CELFpp(k)
          tup._1.foreach(println)
          println(tup._2)
        }else if(chooseAglo==3){
          print("Please enter the heuristic element h(0-1): ")
          val h = Console.readDouble()
          if(h<0 || h>1){
            println("wrong input")
            sys.exit(-1)
          }
          val p = Console.readDouble()
          Algo.setModel(0,iter,p)
          val tup = Algo.TJT(k,h)
          tup._1.foreach(println)
          println(tup._2)
        }else if(chooseAglo==4){
          print("Please enter the heuristic element h(0-1): ")
          val h = Console.readDouble()
          if(h<0 || h>1){
            println("wrong input")
            sys.exit(-1)
          }
          val p = Console.readDouble()
          Algo.setModel(0,iter,p)
          val tup = Algo.TIMM(k,h)
          tup._1.foreach(println)
          println(tup._2)
        }else if(chooseAglo==5){
          Algo.setModel(0,iter,p)
          val tup = Algo.DegreeDiscount(k)
          tup.foreach(println)
        }else{
          println("Please enter right Number, try again!")
        }
      } else if (chooseType == 3) {
        sys.exit(-1)
      } else {
        println("Please enter right Number, try again!")
      }
    }
  }
}
