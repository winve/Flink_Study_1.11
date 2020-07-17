package com.itcpay.apitest

import com.itcpay.model.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object Example {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // src/main/resources/sensor.txt
    val inputStream: DataStream[String] = env.readTextFile("E:\\Idea\\2019\\Flink_Study_1.11\\src\\main\\resources\\sensor.txt")

    // map成样例类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(", ")
        SensorReading(dataArray(0), dataArray(1), dataArray(2))
      })

    // 1、基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2、基于tableEnv将流装换成表
    val dataTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature)

    // 3.1、调用table api，做转换操作
//    val resultTable: Table = dataTable.select("id, temperature").filter("id == 'sensor_1'")

    val resultTable: Table = dataTable.select($"id", $"temperature").filter($"id" === "sensor_1")

    // 3.2、写SQL实现
    tableEnv.createTemporaryView("dataTable", dataTable)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from dataTable
        |where id = 'sensor_1'
        |""".stripMargin)

    // 4、将表转换成流，打印输出
    val resultStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]
    resultStream.print()

    env.execute("table api example job")
  }

}
