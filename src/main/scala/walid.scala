import org.apache.spark.sql._
import org.apache.spark.sql.types._

object walid {

  def main(args: Array[String]): Unit = {

 /// je test git hahaha
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("prog1").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    import org.apache.spark.sql.functions._
   /* val studf = Seq(
        Row(1, "u1", 5),
        Row(1, "u2", 10),
        Row(2, "u2", 20),
        Row(3, "u1", 7),
        Row(3, "u2", 15),
        Row(4, "u2", 5),
        Row(4, "u3", 10),
        Row(5, "u1", 15),
        Row(6, "u4", 10),
        Row(7, "u5",9),
        Row(8, "u6", 7)) */

    //schema de la table source

    val shemaTable =

      StructType(
        StructField("id", IntegerType, true) ::
        StructField("mois", IntegerType, true) ::
        StructField("nom", StringType, true) ::
        StructField("nombreConnexionACtuel", IntegerType, true) :: Nil)

   // val df = spark.createDataFrame(spark.sparkContext.parallelize(studf), shemaTable)
    // df.show()

    val df = spark
      .read
      .option("header", "false")
      .schema(shemaTable)
      .csv("hdfs:///user/abc/table_kpi/part*")


    val  df1 = df.select("mois","nom","nombreConnexionACtuel")




    df1.createOrReplaceTempView("table1")

    df1.show()
    val df2 =spark.sql(" select * from (select nom as nomp , mois+1 as moisp ,nombreConnexionACtuel as connexionprecedante from table1)" +
                       "where moisp < (select max(mois+1) from table1)")

    df2.createOrReplaceTempView("table2")

    val resultat =  spark.sql(" select * from (select t1.mois, t2.moisp , t1.nom ,t2.nomp,t1.nombreConnexionACtuel, " +
                             "t2.connexionprecedante from" +
                             " table1 t1 full join table2 t2 on t1.mois =t2.moisp and t1.nom = t2.nomp)   ")

      .withColumn("connexionprecedante",
        expr(" case when connexionprecedante IS NOT NULL then connexionprecedante else 0 end "))
      .withColumn("nombreConnexionACtuel",
        expr(" case when nombreConnexionACtuel IS NOT NULL then nombreConnexionACtuel else 0 end "))
      .withColumn("nom",
        expr("""case when nom IS NULL and nomp IS NOT NULL then nomp else nom end"""))
      .withColumn("mois",
        expr("""case when mois IS NULL and moisp IS NOT NULL then moisp else mois end"""))
      .select("mois","nom","nombreConnexionACtuel","connexionprecedante")
      .orderBy("mois")


      resultat.show(50)

  }


}
