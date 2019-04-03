package com.atguigu.gmall1018.dw.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1018.dw.common.constant.GmallConstant
import com.atguigu.gmall1018.dw.common.util.MyEsUtil
import com.atguigu.gmall1018.dw.realtime.bean.StartUpLog
import com.atguigu.gmall1018.dw.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object StartUpLogApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("startUplog")
    val ssc = new StreamingContext(new SparkContext(sparkConf), Seconds(5))
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_STARTUP, ssc)

    //    recordDStream.map(_.value()).foreachRDD(rdd=>{
    //      println(rdd.collect().mkString("\n"))
    //    })

    //将日志数据结构化

    val startupLogDStream: DStream[StartUpLog] = recordDStream.map(_.value()).map(jsonString => {

      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      val ts: Long = startUpLog.ts
      val date: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(ts))
      val datesArr: Array[String] = date.split(" ")
      startUpLog.logDate = datesArr(0)
      val timeArry: Array[String] = datesArr(1).split(":")
      startUpLog.logHour = timeArry(0);
      startUpLog.logHourMinute = timeArry(0) + ":" + timeArry(1)
      startUpLog
    }
    )

    //判断当日用户是否存在,es无法去重
    //用transfer算子进行定期检查
    val startupLogFilterDstream: DStream[StartUpLog] = startupLogDStream.transform(rdd => {
      println(s"过滤前=${rdd.count()}")
      val jedis = new Jedis("hadoop102", 6379)
      val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val key = "dau:" + today
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      val filterRdd: RDD[StartUpLog] = rdd.filter(startupLog => !dauBC.value.contains(startupLog.mid))
      println(s"过滤后=${filterRdd.count()}")
      filterRdd
    }
    )

    //需要对五秒内在去重,防止5秒内产生大量重复数据
    val distinctDstream: DStream[StartUpLog] = startupLogFilterDstream.map(x=>(x.mid,x)).groupByKey().flatMap(x=>x._2.take(1))




    //写入redis 和es
    distinctDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(startupLog=>{
      val jedis = new Jedis("hadoop102",6379)
        //新建一个list去接收startupLog迭代器中的元素 然后转为list放入批处理中
        val listBuffer = new ListBuffer[StartUpLog]
        for (elem <- startupLog) {
          val key = "dau:"+ elem.logDate
          jedis.sadd(key,elem.mid)
          listBuffer+=elem
        }

        // 写入es
        MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_DAU,listBuffer.toList)




        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
