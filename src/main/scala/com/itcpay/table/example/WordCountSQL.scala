package com.itcpay.table.example

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object WordCountSQL {

  def main(args: Array[String]): Unit = {
    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))

    // register the DataSet as a view "WordCount"
    tEnv.createTemporaryView("WordCount", input, $"word", $"frequency")

    // run a SQL query on the Table and retrieve the result as a new Table
    val table = tEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")

    table.toDataSet[WC].print()
  }

  case class WC(word: String, frequency: Long)

}
