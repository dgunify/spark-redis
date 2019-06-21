package com.dgunify;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.redislabs.provider.redis.*;

import scala.Tuple2;
import scala.collection.immutable.List;

/**
 * @author dgunify
 * 使用spark-sql读取数据存入redis
 */
public class ReadDataPutRedis {
	public static void main(String[] args) {
		try {
			SparkConf conf = new SparkConf().setMaster("spark://hadoop0:7077").setAppName("sparkRedisTest");
			conf.set("redis.host", "127.0.0.1");//host,随便一个节点，自动发现
			conf.set("redis.port", "6379");//端口号，不填默认为6379
			conf.set("redis.db","0");//数据库设置
			conf.set("redis.timeout","2000");//设置连接超时时间
			
			Builder bu =  SparkSession.builder().config(conf);
			SparkSession ss = bu.getOrCreate();
			SparkContext sc = ss.sparkContext();
			DataFrameReader read = ss.read();
			//spark-sql read db data
			//使用spark-sql读取数据库数据
			Dataset<org.apache.spark.sql.Row> ds = read.format("jdbc")
			.option("url","jdbc:oracle:thin:@127.0.0.1:1521:orcl")
			.option("driver","oracle.jdbc.driver.OracleDriver")
			.option("dbtable","users")
			.option("user","admin")
			.option("password","admin123").load();
			
			JavaRDD<Row> javaRDD = ds.toJavaRDD();
			//to rdd,key value
			//将数据库数据转换为key value
			JavaRDD<Tuple2<String, String>> userRDD =  javaRDD.map(new Function<Row, Tuple2<String, String>>() {
				public Tuple2<String, String> call(Row row) throws Exception {
					String key = row.getAs("ID");
					String value = row.getAs("NAME");
					return new Tuple2<String,String>(key,value);
				}
			});
			RedisContext rc = new RedisContext(sc);
			Iterator<Tuple2<String, String>> it1 = userRDD.collect().iterator();
			scala.collection.Iterator<Tuple2<String, String>> it2 = scala.collection.JavaConversions.asScalaIterator(it1);
			RedisEndpoint initialHost = new RedisEndpoint(conf);
			RedisConfig redisConfig = new RedisConfig(initialHost);
			//存入redis
			rc.setKVs(it2, 0, redisConfig);
			System.out.println("ok");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
