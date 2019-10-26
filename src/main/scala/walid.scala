import org.apache.spark.sql._
import org.apache.spark.sql.types._

object walid {

  def main(args: Array[String]): Unit = {



    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("prog1").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val studf = Seq(
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
        Row(8, "u6", 7))

    //schema de la table source


    println("************** TEST de clacul de KPI sur un mois ne arrire avec sql")

    val shemaTable =

      StructType(
      //  StructField("id", IntegerType, true) ::
        StructField("mois", IntegerType, true) ::
        StructField("nom", StringType, true) ::
        StructField("nombreConnexionACtuel", IntegerType, true) :: Nil)

   val df = spark.createDataFrame(spark.sparkContext.parallelize(studf), shemaTable)
    // df.show()

   /* val df = spark
      .read
      .option("header", "false")
      .schema(shemaTable)
      .csv("hdfs:///user/abc/table_kpi/part*")*/


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

    println("************ resultat full join *****************")
    resultat.show(50)





    println("************ TEST UNION pour calculer des KPI sur deux en arriére  *****************")


    val maxMois = spark.sql("select max(mois) from table1").collect().toList.head


    val df3 = df1.withColumn("mois", $"mois"+1)
                 .withColumn ("nombreConnexionACtuel", lit(0))
                 .filter($"mois" <= maxMois(0))

    val df3_plus_2= df1.withColumn("mois", $"mois"+2)
                       .withColumn ("nombreConnexionACtuel", lit(0))
                        .filter($"mois" <= maxMois(0))



     val df4=  df1.union(df3).orderBy("mois").dropDuplicates("nom","mois")
     val df4_plus_2 =  df1.union(df3_plus_2).orderBy("mois").dropDuplicates("nom","mois")

      val v0 = df4.union(df4_plus_2).orderBy("mois").dropDuplicates("nom","mois")



     val v1 = v0.withColumn("mois", $"mois" + 1)
                  .withColumnRenamed("nombreConnexionACtuel","nombreConnexionPrecedente")

    val v2 = v0.withColumn("mois", $"mois" + 2)
               .withColumnRenamed("nombreConnexionACtuel","nombreConnexionPrecedente1")


    val df6 = v0.join(v1, Seq("mois", "nom"),joinType = "left")
                .join(v2, Seq("mois", "nom"),joinType = "left")

                .withColumn("nombreConnexionACtuel",
                   expr("case when nombreConnexionACtuel IS NOT NULL then nombreConnexionACtuel  else 0 end  "))
                 .withColumn("nombreConnexionPrecedente",
                   expr("case when nombreConnexionPrecedente IS NOT NULL then nombreConnexionPrecedente  else 0 end  "))
                  .withColumn("nombreConnexionPrecedente1",
                 expr("case when nombreConnexionPrecedente1 IS NOT NULL then nombreConnexionPrecedente1  else 0 end  "))

             .orderBy("mois")





    println("************ donne entre *****************")
    df1.show(100)

    println("************ donne plus + mois *****************")
    df3.show()

    println("************ donne union *****************")

    v0.show(100)

    println("************ donne union +1 *****************")
    v2.show(100)

    println("************ donne jointure *****************")
    df6.show(100)




  }


}
