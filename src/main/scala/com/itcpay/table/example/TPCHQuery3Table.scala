package com.itcpay.table.example

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object TPCHQuery3Table {

  def main(args: Array[String]): Unit = {

    // set filter date
    val date = "1995-03-12".toDate

    // get execution environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)

    val lineitems: Table = getLineitemDataSet(env)
      .toTable(tableEnv, 'id, 'extdPrice, 'discount, 'shipDate)
      .filter('shipDate.toDate > date)

  }

  case class Lineitem(id: Long, extdPrice: Double, discount: Double, shipDate: String)

  private var lineitemPath: String = _
  private var customerPath: String = _
  private var ordersPath: String = _
  private var outputPath: String = _

  private def getLineitemDataSet(env: ExecutionEnvironment): DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
      lineitemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 5, 6, 10)
    )
  }

}
