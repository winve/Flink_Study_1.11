package com.itcpay.table.example

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object StreamSQLExample {

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val planner: String = if (params.has("planner")) params.get("planner") else "blink"

    // set up execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // use blink planner in streaming mode
    val tableEnv: StreamTableEnvironment = if (planner == "blink") {
      val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()
      StreamTableEnvironment.create(env, settings)
    } else if (planner == "flink") { // use flink planner in streaming mode
      val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
        .useOldPlanner()
        .inStreamingMode()
        .build()
      StreamTableEnvironment.create(env, settings)
    } else {
      System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
        "where planner (it is either flink or blink, and the default is blink) indicates whether the " +
        "example uses flink planner or blink planner.")
      return
    }

    val orderA: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)
    ))

    val orderB: DataStream[Order] = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))

    // convert DataStream to Table
    val tableA: Table = tableEnv.fromDataStream(orderA, 'user, 'product, 'amount)
    // register DataStream as Table
    tableEnv.createTemporaryView("OrderB", orderB, 'user, 'product, 'amount)

    // union the two table
    val result: Table = tableEnv.sqlQuery(
      s"""
         |SELECT * FROM $tableA WHERE amount > 2
         |UNION ALL
         |SELECT * FROM OrderB WHERE amount < 2
         |""".stripMargin)

    result.toAppendStream[Order].print()

    env.execute()
  }

  case class Order(user: Long, product: String, amount: Int)

}
