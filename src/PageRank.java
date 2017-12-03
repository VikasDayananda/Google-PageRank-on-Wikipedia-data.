package org.vikas;

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.util.regex.Pattern;
import java.util.regex.Matcher; 
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FileStatus; 
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank extends Configured implements Tool {
	
	//private static String inputPath="";
	//private static String outputPath="";
	public static final Pattern TITLE_REGEX = Pattern.compile("<title>(.+?)</title>");  //Pattern for extracting title in input file
	public static final Pattern LINK_REGEX = Pattern.compile("<l>(.+?)</l>");   		//Pattern for outlinks title in input file
	public static final Pattern PR_REGEX = Pattern.compile("<pr>(.+?)</pr>");			//Pattern for pagerank title in input file

	private static final Logger LOG = Logger.getLogger(PageRank.class);

	/*public static void main(String[] args) throws Exception {
		int noOfRuns=10;
		int res=0;
		inputPath=args[0];
		for(int i=1;i<=noOfRuns;i++){
			if(i!=1){
				inputPath=outputPath;
			}
			outputPath=args[1]+i;
			res = ToolRunner.run(new Configuration(),new PageRank(), args);
		}
		
		System.exit(res);
	}*/

	public int run(String[] args) throws Exception {
		
		 // Configure and create the job class.
		Job job = Job.getInstance(getConf(), "PageRank");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		//Store output as Sequence file.
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	//Define Map Operations.
	public static class Map extends Mapper<Text, Text, Text, Text> {

		private Text mapKeyOutput = new Text();  

		public void map(Text key, Text lineText, Context context)
		throws IOException, InterruptedException {
			
			String line = lineText.toString();
			
			if(!line.isEmpty()){
				
				//Emit (URL, pr-url_list).
				context.write(key, lineText);
				
				//Extract  pagerank.
				Matcher matcher_pr= PR_REGEX.matcher(line);
				matcher_pr.find();
				Double initial_pr=Double.parseDouble(matcher_pr.group(1));
				
				String[] parts1 = line.split("<pr>");
				String URL= parts1[0];
				
				//Extract Outlinks.
				Matcher matcher= LINK_REGEX.matcher(parts1[1]);
				final List<String> outlinks = new ArrayList<String>();
				while(matcher.find()){
					
					outlinks.add(matcher.group(1));
				}
				for(String link:outlinks){
					mapKeyOutput  = new Text(new StringBuilder().append(link).toString());
					Double link_pr=initial_pr/outlinks.size();
					//For each u in url_list, emit (u, cur_rank/|url_list|).
					context.write(new Text(link), new Text(link_pr.toString()));			
				}
			}
		}
	}

	//Define Reduce operations.
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {	
			
			String key=word.toString();
			double sum=0;
			StringBuffer valueBuffer=new StringBuffer();
			StringBuffer finalBuffer=new StringBuffer();
			boolean isPage=false;
			
			for(Text value:values){

				Matcher matcher_pr= PR_REGEX.matcher(value.toString());
			
				if(!matcher_pr.find()){
					
					//Sum all pagerank values of the url.
					sum+=Double.parseDouble(value.toString());
				}
				else{
					
					Matcher matcher_link= LINK_REGEX.matcher(value.toString());
					
					isPage=true;
					while(matcher_link.find()){
						
						valueBuffer.append("<l>").append(matcher_link.group(1)).append("</l>");
					} 
					
				}
			}
			//page rank computation using PR(A) = (1 -d) + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))
			sum=0.15+(sum*0.85);
			
			//Append updated pagerank.
			finalBuffer.append("<pr>").append(sum).append("</pr>").append(valueBuffer);
			//checking whether the record is present in the title
			if(isPage){
				//  Format: "page   <pr>upadted pagerank</pr> <l>x</l> <l>y</l>....." where x ,y .. are outlinks for page.
				context.write(new Text(key), new Text(finalBuffer.toString()));
			}
		}
	}
}
