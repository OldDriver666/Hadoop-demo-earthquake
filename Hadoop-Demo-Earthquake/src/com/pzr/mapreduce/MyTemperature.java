package com.pzr.mapreduce;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * 查询最大震级
 * @author pzr
 *
 */
public class MyTemperature {

	public static void main(String args[]) throws IOException{

		System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0");
		JobConf conf = new JobConf(MyTemperature.class);
		conf.setJobName("myJob");
		
		Long fileName = (new Date()).getTime();
		
		FileInputFormat.addInputPath(conf, new Path("hdfs://192.168.215.11:9000/user/earthquake_info.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.215.11:9000/user/"+fileName));
		
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		JobClient.runJob(conf);
	}
}
