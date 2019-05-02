package com.recommend

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

//  在线相似度计算
object onlineRecomm {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Recommend")
    val ssc = new StreamingContext(conf, Seconds(60))

    // todo 缓存代理，Kafka集群中的一台或多台服务器统称broker.需要设置
    val brokerList = ""
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g100",
      //这个代表，任务启动之前产生的数据也要读
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    // 设置topic(topic和中间件确认)
    val topics = Array("recommend")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // 保存offset
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

//    todo 消息处理 需要知道消息格式做相应的解析（和中间件的同学确认） 根据电影ID计算相似的20部电影可看下面的例子,计算后的movieRDD 存到hive中
    stream.map(record=>{

      // 从hdfs中获取相似度矩阵 获取与电影ID相似的电影
      // 存储到hive 数据库


    })

    //    // 在线相似度计算举例 例如计算电影ID为12的相似电影
    //    val movieId = "12"
    //    val movieList = rdd.lookup(movieId)
    //    val sc = sparkSession.sparkContext
    //    val sortedList = sc.parallelize(movieList).filter(_._1 != movieId).sortBy(_._2,false).take(20)
    //    val movieRdd = sc.parallelize(sortedList).map(each => (movieId,each._1)).groupByKey()
    //    movieRdd.foreach(println)


    ssc.start()
    ssc.awaitTermination()

  }

}
