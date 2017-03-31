package mcad;

//update file...use Htable instead of HashSet

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
import java.util.HashSet;
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

import mcad.DOIUnionKeyphrasesCitationContext.FilterMapper;
import mcad.DOIUnionKeyphrasesCitationContext.PaperEntryReducer;
import mcad.UniqueKeyphrasesCount.InputMapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class ListKeywordsFromContext {
	public static class InputMapper extends TableMapper<Text, Text> {
		
		//HashMap<Text, Text> DOItoContext = new HashMap<>();
		HashSet<String> globalKeyphraseSet = new HashSet();
		
		private HTable htable;
		private Configuration config;
		
		protected void setup(TableMapper<Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "172.17.25.18");
			config.set("hbase.zookeeper.property.clientPort", "2183");
			htable = new HTable(config, "GlobalKeyphrases");
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("Keyphrases"), Bytes.toBytes("Keyphrases"));
			ResultScanner scanner = htable.getScanner(scan);
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				String rowId = Bytes.toString(result.getRow());
				String keyphrase = Bytes.toString(result.getValue(Bytes.toBytes("Keyphrases"), Bytes.toBytes("Keyphrases")));
				globalKeyphraseSet.add(keyphrase);
			}
			
		}
		
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			
			String DOI = Bytes.toString(row.get());
			
			String mergedContext = Bytes.toString(value.getValue(Bytes.toBytes("CitationContext"), Bytes.toBytes("mergedContext")));
			
			String[] contexts = mergedContext.split(";;");
			
			String keyphrases = "";
			for (String phrases : globalKeyphraseSet) {
				if(mergedContext.contains(phrases.trim())) {
					keyphrases += ";" + phrases;
				}
				
			}
			
			context.write(new Text(DOI), new Text(keyphrases));
			
		}
	}
	
	
	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text doi, Iterable<Text> phrases, Context context)
				throws IOException, InterruptedException {

			String paperdoi = doi.toString().trim();
			String contextkeywordList = "";
			for (Text phrase : phrases) {
				contextkeywordList = phrase.toString();
			}
			
			Put put = new Put(Bytes.toBytes(paperdoi));
			put.add(Bytes.toBytes("DOI"), Bytes.toBytes("DOI"), Bytes.toBytes(paperdoi));
			put.add(Bytes.toBytes("TermList"), Bytes.toBytes("TermList"), Bytes.toBytes(contextkeywordList));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(paperdoi)), put);

		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		
		Job job = Job.getInstance(conf, "ListKeywordsFromContext");
		job.setJarByClass(ListKeywordsFromContext.class);
		job.setMapperClass(FilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PaperEntryReducer.class);
		
		Scan scan = new Scan();
		scan.setCaching(500);      
		scan.setCacheBlocks(false); 
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//FileInputFormat.setInputPaths(job, new Path(args[0]));	// "globalKeyphrases.txt"
		
		TableMapReduceUtil.initTableMapperJob("DOICitationContext", scan, InputMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("DOITermList", PaperEntryReducer.class, job);
		
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}

}