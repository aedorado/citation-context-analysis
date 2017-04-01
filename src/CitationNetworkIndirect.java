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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class CitationNetworkIndirect {
	public static class InputMapper extends TableMapper<Text, Text> {
		
		private HTable htablePaperBagofWords;
		private Configuration config;
		private static int threshold = 5;

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
			
			String fromPaper = Bytes.toString(value.getValue(Bytes.toBytes("fromPaper"), Bytes.toBytes("fromPaper")));
			String toPapersDirectlist = Bytes.toString(value.getValue(Bytes.toBytes("toPapersDirect"), Bytes.toBytes("toPapersDirect")));
			
			Get doiFrom = new Get(Bytes.toBytes(fromPaper));
			Result doirowResult = htablePaperBagofWords.get(doiFrom);
			
			String[] toPapersDirect = toPapersDirectlist.split(",");
			HashSet<String> toPapersSet = new HashSet();
			for (String toPaper : toPapersDirect){
				toPapersSet.add(toPaper);
			}
			
			if(!doirowResult.isEmpty()) {
				String fromPaperTerms = Bytes
						.toString(doirowResult.getValue(Bytes.toBytes("keyphrases"), Bytes.toBytes("paperKeyphrases"))) ;
				fromPaperTerms += ";"+Bytes
						.toString(doirowResult.getValue(Bytes.toBytes("citationContext"), Bytes.toBytes("citationContextTerms")));
				String[] fromTerms = fromPaperTerms.split(";");
						
				Scan scan = new Scan();
		        ResultScanner scanner = htablePaperBagofWords.getScanner(scan);
		        Result r;
		        String toDOIindirectList = "";
		        
		        while (((r = scanner.next()) != null)) {
		            byte[] rowkey = r.getRow();
		            String DOI = Bytes.toString(rowkey);            
		            
		            if (!toPapersSet.contains(DOI) && !DOI.equals(fromPaper)){
		            	byte[] b_keyPhrases = r.getValue(Bytes.toBytes("keyphrases"), Bytes.toBytes("paperKeyphrases"));
			            byte[] b_contextTerms = r.getValue(Bytes.toBytes("citationContext"), Bytes.toBytes("citationContextTerms"));
			            
			            String keyPhrases = Bytes.toString(b_keyPhrases);
			            String contextTerms = Bytes.toString(b_contextTerms);
			            String toTermList = keyPhrases+";"+contextTerms;
			            
			            String[] toTerms = toTermList.split(";");
			            int count = 0;
			            for (String fromTerm : fromTerms){
			            	fromTerm = fromTerm.trim();
			            	for(String toTerm : toTerms){
			            		toTerm = toTerm.trim();
			            		if((toTerm != "") && (fromTerm != "") && (toTerm.equals(fromTerm))){
			            			count++;
			            		}
			            	}
			            	if (count == threshold){
			            		toDOIindirectList += DOI+",";
			            		break;
			            	}
			            }
			            
		            }
		        }
		        
		        scanner.close();
				context.write(new Text(fromPaper), new Text(toDOIindirectList));
			}
			//htablePaperBagofWords.close();
		}	
	}
	
	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text doi, Iterable<Text> phrases, Context context)
				throws IOException, InterruptedException {

			String fromPaper = doi.toString().trim();
			String toPapersIndirect = "";
			for (Text toPaperIndirect : phrases) {
				toPapersIndirect = toPaperIndirect.toString();
			}
			
			
			Put put = new Put(Bytes.toBytes(fromPaper));
			put.add(Bytes.toBytes("toPapersIndirect"), Bytes.toBytes("toPapersIndirect"), Bytes.toBytes(toPapersIndirect));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(fromPaper)), put);

		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		
		Job job = Job.getInstance(conf, "CitationNetworkIndirect");
		job.setJarByClass(CitationNetworkIndirect.class);
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
		
		TableMapReduceUtil.initTableMapperJob("ContextCitationNetwork", scan, InputMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("ContextCitationNetwork", PaperEntryReducer.class, job);
		
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}

}


