package com.itcpay.table.example

import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api._

/**
 * convert DataSets to Tables
 * apply group, aggregate, select, and filter operations
 */
object WordCountTable {

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)

    val inputSet: DataSet[WC] = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val expr: Table = inputSet.toTable(tableEnv)

    val result: DataSet[WC] = expr.groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === 2)
      .toDataSet[WC]

    result.print()
  }

  case class WC(word: String, frequency: Long)

}
