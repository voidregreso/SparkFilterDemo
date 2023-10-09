package com.ernesto.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class JsonFilterQueryMain {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        // bigdata001 es el nombre de host del nodo NameNode.
        SparkConf conf = new SparkConf();
        conf.setAppName("JsonAggregation")
                .setMaster("spark://bigdata001:7077")
                .set("spark.driver.memory", "1g")
                .set("spark.driver.cores", "2")
                .set("spark.driver.host", "192.168.1.104")
                .setJars(new String[]{"D:\\BigData\\SparkFilterDemo\\build\\libs\\SparkFilterDemo.jar"});

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Leer ficheros JSON
        String jsonPath = "hdfs://bigdata001:8020/user/sp_case/data.json";
        Dataset<Row> df = spark.read()
                .option("multiline", true) // Hay un problema: tiene que estar en modo multilínea.
                .json(jsonPath);

        // Cálculo de la antigüedad media
        Dataset<Row> avgAge = df.agg(avg("age").alias("average_age"));

        // Encontrar todas las aficiones que son iguales
        Dataset<Row> hobbiesDF = df.withColumn("common_hobby", explode(col("hobbies")))
                .groupBy("common_hobby")
                .agg(count("name").alias("count"))
                .filter(col("count").equalTo(df.count())) // Elige sólo aficiones que tengan todas las personas
                .select("common_hobby")
                .distinct();

        // Exportar edad media a HDFS
        String avgAgeOutputPath = "hdfs://bigdata001:8020/user/sp_case/avg_age";
        avgAge.coalesce(1).write().json(avgAgeOutputPath);

        // Salida de la misma todas las aficiones a HDFS
        String coHobOutputPath = "hdfs://bigdata001:8020/user/sp_case/co_hob";
        hobbiesDF.coalesce(1).write().json(coHobOutputPath);

        // Detener una sesión Spark
        spark.stop();
    }
}
