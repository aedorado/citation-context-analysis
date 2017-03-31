package mcad;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class UniqueKeyphrasesCount {
	
	public static int tableRowId = 1;
	
	public static class InputMapper extends TableMapper< Text, IntWritable> {
		private final IntWritable ONE = new IntWritable(1);
	   	private Text text = new Text();

	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	        	String val = new String(value.getValue(Bytes.toBytes("keyphrases"), Bytes.toBytes("paperKeyphrases")));
	        	String keywords[] = val.split(";");
	        	for(String keyword : keywords) {
	        		text.set(keyword);
	        		context.write(text, ONE);		
	        	}

	   		}
	
		}
	
	public static class PaperEntryReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

		public void reduce(Text keyword, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		
			String keyphrase = keyword.toString().trim();
    		Put put = new Put(Bytes.toBytes(Integer.toString(tableRowId)));
			put.add(Bytes.toBytes("Keyphrases"), Bytes.toBytes("Keyphrases"), Bytes.toBytes(keyphrase));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(Integer.toString(tableRowId))), put);
			tableRowId++;
		}
		
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		
		Scan scan = new Scan();
		scan.setCaching(500);      
		scan.setCacheBlocks(false); 
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "UniqueKeyphrasesCount");
		job.setJarByClass(UniqueKeyphrasesCount.class);
//		job.setMapperClass(InputMapper.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		
		//FileInputFormat.setInputPaths(job, new Path(args[0]));	// "metadata.txt"
		//FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		TableMapReduceUtil.initTableMapperJob("PaperBagofWords", scan, InputMapper.class, Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("GlobalKeyphrases", PaperEntryReducer.class, job);
		
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}

}
