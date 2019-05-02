package com.recommend

import breeze.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SQLContext}
import scala.reflect.runtime.universe
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, VectorAssembler}
import org.apache.spark.ml.linalg.{SparseVector => SV}

// 离线计算相似度矩阵
object contentRecomm {

  case class movie(mid: String, vec: SV)

  case class SimMovie(mid: Int, simId: Int, similarity: Double)

  def main(args: Array[String]): Unit = {

    // todo 路径需要改成hdfs
    val localpath = "D:\\data\\filmsSample.csv"
    val simPath = "D:\\data\\SimSample"

    // todo master要根据集群的IP配置
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark content recommend")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.option("header", true).csv(localpath)

    //缺失值处理
    val dataDF = data.na.fill("unknown").toDF()
    //    dataDF.show()

    val splitData = dataDF.map(line => {
      val actorArr = line.getString(10).split(",")
      val countryArr = line.getString(11).split(",")
      val tagArr = line.getString(12).split(",")
      (line.getString(0), actorArr, countryArr, tagArr)
    }).toDF("fid", "actor", "country", "tags")


    // 转换 拼接 计算相似度 排序 存储 返回结果
    val cvmodel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("tags")
      .setOutputCol("tagsVector")
      .fit(splitData)

    val cvmodel1: CountVectorizerModel = new CountVectorizer()
      .setInputCol("country")
      .setOutputCol("countryVector")
      .fit(splitData)

    val cvmodel2: CountVectorizerModel = new CountVectorizer()
      .setInputCol("actor")
      .setOutputCol("actorVector")
      .fit(splitData)

    val df = cvmodel.transform(splitData).select("fid", "tagsVector").toDF()
    val df1 = cvmodel1.transform(splitData).select("fid", "countryVector").toDF()
    val df2 = cvmodel2.transform(splitData).select("fid", "actorVector").toDF()

    val finalDF = df.join(df1, "fid").join(df2,"fid")


    val assembler = new VectorAssembler()
      .setInputCols(Array("tagsVector", "countryVector","actorVector"))
      .setOutputCol("features")
    val output = assembler.transform(finalDF)

    val rdd = output.select("fid", "features").rdd.cache()

    def parseRating(row: Row): movie = {
      movie(row.getString(0), row.get(1).asInstanceOf[SV])
    }

    val rdd1 = rdd.map(parseRating)
    //    rdd1.foreach(println)

    import breeze.linalg._
    //  计算相似度
    def cosineSimilarity(vec1: SparseVector[Double], vec2: SparseVector[Double]): Double = {
      val cosSim = vec1.dot(vec2) / (norm(vec1) * norm(vec2))
      cosSim
    }

    val rdd2 = rdd1.cartesian(rdd1)
    val rdd3 = rdd2.map(f => {
      val sv1 = new SparseVector[Double](f._1.vec.indices, f._1.vec.values, f._1.vec.size)
      val sv2 = new SparseVector[Double](f._2.vec.indices, f._2.vec.values, f._2.vec.size)
      (f._1.mid, (f._2.mid, cosineSimilarity(sv1, sv2)))
    }).cache()

    //  相似度矩阵
    val rdd4 = rdd3.map(x=>SimMovie(x._1.toInt,x._2._1.toInt,x._2._2)).toDF()
    rdd4.show()

    // 存入到hdfs 格式csv或者txt
//    rdd4.write.format("csv").save(simPath)
    val rdd5 = rdd4.rdd
    rdd5.saveAsTextFile(simPath)
  }

}
