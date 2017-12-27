package com.pzr.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text,Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		
		String str = value.toString();
		if(str != null && str.length() > 2 && 0 != str.substring(0,2).trim().length()){
			str = str.replaceAll(" +", " ");
			String data[] = str.split(" "); 
			String date = data[0]; //日期
			String time = data[1]; //时间
			String lng = data[2]; //经度
			String lat = data[3]; //纬度
			String depth = data[4]; //深度（功力）
			String magnitude = data[5]; //震级
			String address = data[6];//参考地址
			
			output.collect(new Text(date.substring(0,7)), new Text(magnitude+"#"+address));//每一个月的最大
//			output.collect(new Text(date.substring(0,4)), new Text(magnitude+"#"+address));//每一个年的最大
//			output.collect(new Text("allMax"), new Text(magnitude+"#"+address));//所有的最大
			
		}
	}
	
}