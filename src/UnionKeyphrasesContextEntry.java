package mcad;                                        //yet to be validated

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

public class UnionKeyphrasesContextEntry {
	public static class InputMapper extends TableMapper<Text, Text> {
		
		private HTable htablePaperBagofWords;
		private Configuration config;

		@Override
		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "172.17.25.18");
			config.set("hbase.zookeeper.property.clientPort", "2183");
			try {
				htablePaperBagofWords = new HTable(config, "PaperBagofWords");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			
			String DOI = Bytes.toString(value.getValue(Bytes.toBytes("DOI"), Bytes.toBytes("DOI")));
			String contextTermlist = Bytes.toString(value.getValue(Bytes.toBytes("TermList"), Bytes.toBytes("TermList")));
			
			Get doirow = new Get(Bytes.toBytes(DOI));
			Result doirowResult = htablePaperBagofWords.get(doirow);
			if(!doirowResult.isEmpty() && !contextTermlist.isEmpty()) {
				String keyphrase = Bytes
						.toString(doirowResult.getValue(Bytes.toBytes("keyphrases"), Bytes.toBytes("paperKeyphrases")));
				String[] terms = contextTermlist.split(";");
				String UniqueTermList = "";
				if(keyphrase != null) {
					String[] keyphrases = keyphrase.split(";");
					for (String term : terms) {
						int flag = 0;
						if(term.trim().isEmpty()) continue;
					
						for(String phrase : keyphrases) {
							if(term.trim().equals(phrase)) {
								flag = 1;
								break;
							}
						}
					
						if(flag == 0) {
							UniqueTermList += term + ";";
						}
					
					}
				
					context.write(new Text(DOI), new Text(UniqueTermList));
				}
				else {
					for (String term : terms) {
						if(term.trim().isEmpty()) continue;
						UniqueTermList += term.trim() + ";";
					}
				
					context.write(new Text(DOI), new Text(UniqueTermList));
				}
				
			}
			
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
			put.add(Bytes.toBytes("citationContext"), Bytes.toBytes("citationContextTerms"), Bytes.toBytes(contextkeywordList));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(paperdoi)), put);

		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		
		Job job = Job.getInstance(conf, "UnionKeyphrasesContextEntry");
		job.setJarByClass(UnionKeyphrasesContextEntry.class);
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
		
		TableMapReduceUtil.initTableMapperJob("DOITermList", scan, InputMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("PaperBagofWords", PaperEntryReducer.class, job);
		
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}

}


