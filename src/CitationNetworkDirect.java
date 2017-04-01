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

public class CitationNetworkDirect {
	public static class InputMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] cols = value.toString().split("\\*\\*\\*");
			String fromDOI = cols[0].trim();
			String toDOI = cols[1].trim();
		
			context.write(new Text(fromDOI), new Text(toDOI));
			
		}
	}
	
	
	public static class CitationNetworkDirectReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text fromDOI, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String fromPaper = fromDOI.toString().trim();
			String mergedToPaperDirect = "";
			for (Text val : values) {
				mergedToPaperDirect += val.toString() + ",";
			}
			
			Put put = new Put(Bytes.toBytes(fromPaper));
			put.add(Bytes.toBytes("fromPaper"), Bytes.toBytes("fromPaper"), Bytes.toBytes(fromPaper));
			put.add(Bytes.toBytes("toPapersDirect"), Bytes.toBytes("toPapersDirect"), Bytes.toBytes(mergedToPaperDirect));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(fromPaper)), put);

		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		
		Job job = new Job(conf, "CitationNetworkDirect");
		job.setJarByClass(CitationNetworkDirect.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));	// "filteredCitations.txt"

		TableMapReduceUtil.initTableReducerJob("ContextCitationNetwork", CitationNetworkDirectReducer.class, job);
		job.setReducerClass(CitationNetworkDirectReducer.class);
		job.waitForCompletion(true);
	}

}
