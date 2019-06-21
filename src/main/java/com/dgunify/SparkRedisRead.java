package com.dgunify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import com.redislabs.provider.redis.RedisEndpoint;

import scala.Tuple2;

/**
 * @author dgunify
 * spark-redis读取redis数据
 */
public class SparkRedisRead {
	public static void main(String[] args) {
		try {
			SparkConf conf = new SparkConf().setMaster("spark://hadoop0:7077").setAppName("sparkRedisTest");
			conf.set("redis.host", "127.0.0.1");//host,随便一个节点，自动发现
			conf.set("redis.port", "6379");//端口号，不填默认为6379
			conf.set("redis.db","0");//数据库设置
			conf.set("redis.timeout","2000");//设置连接超时时间
			JavaSparkContext ctx = new JavaSparkContext(conf);
			SparkContext sparkContext = ctx.toSparkContext(ctx);
			RedisContext redisC = new RedisContext(sparkContext);
			RedisEndpoint initialHost = new RedisEndpoint(conf);
			RedisConfig redisConfig = new RedisConfig(initialHost);
			
			//读取redis 
			RDD<Tuple2<String, String>> bigdataRDD = redisC.fromRedisKV("bigdata:*", 1, redisConfig);
			JavaRDD<Tuple2<String, String>> jbigdataRDD = bigdataRDD.toJavaRDD();
			//可以将数据进行清洗等操作
			//jbigdataRDD.mapPartitions(f);
			
			List<Tuple2<String, String>> rbigdataList = jbigdataRDD.collect();
			final Map<String,String> parMap = new HashMap<String, String>();
			//将redis 转换为map
			for(Tuple2<String, String> tuple2 : rbigdataList) {
				String key = tuple2._1;
				String value = tuple2._2;
				parMap.put(key, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
