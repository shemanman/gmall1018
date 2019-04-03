package com.atguigu.gmall1018.dw.common.util
import java.util
import java.util.Objects

import com.google.gson.GsonBuilder
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}


object MyEsUtil {
  private val ES_HOST = "http://hadoop103"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

  //批处理
  def executeIndexBulk(indexName:String,list:List[Any]): Unit ={
    val jestClient = getClient
    val builder = new Bulk.Builder
    //把list循环加到批处理中
    builder.defaultIndex(indexName).defaultType("_doc")
    for (doc <- list) {
      val index: Index = new Index.Builder(doc).build()
      builder.addAction(index)
    }
    val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(builder.build()).getItems
    println(s"保存ES = ${items.size()}  条")
    close(jestClient)
  }

}
