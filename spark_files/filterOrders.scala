import org.apache.spark.sql.SparkSession

object filterOrders {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val ordersDf = spark.read.format("csv").option("header", true).option("inferSchema", true)
      .option("path", args(0)).load

    val closedOrdersDf = ordersDf.filter("order_status = 'CLOSED'")
    closedOrdersDf.write.format("csv").option("path", args(1)).save() //airflow_output
  }
}
