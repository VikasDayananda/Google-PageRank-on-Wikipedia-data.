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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

  

public class LinkGraph extends Configured implements Tool {

	public static final String lineCount_loc="output/vdayanan/linecount_temp";        	//Temporary Storage for Lines Count
	public static final Pattern TITLE_REGEX = Pattern.compile("<title>(.+?)</title>");	//Pattern for recognizing title in wiki file
	public static final Pattern LINK_REGEX = Pattern.compile("\\[\\[(.*?)\\]\\]");		//Pattern for recognizing outlink in wiki file
	
	private static final Logger LOG = Logger .getLogger(LinkGraph.class);
	
	public int run( String[] args) throws  Exception {

		//Delete if temp location already exists.
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(new Path(lineCount_loc))) {
			fs.delete(new Path(lineCount_loc), true);
		}
		 // Configure and create the job class.
		Job countjob  = Job .getInstance(getConf(), " count ");
		countjob.setJarByClass( this .getClass());
		FileInputFormat.addInputPaths(countjob,  args[0]);
		FileOutputFormat.setOutputPath(countjob,  new Path(lineCount_loc));
		// Set Map class name for mappers.
		countjob.setMapperClass( CountMap .class);
		// Set Reduce class name for reducers.
		countjob.setReducerClass( CountReduce .class);
		countjob.setMapOutputKeyClass( Text .class);
		countjob.setMapOutputValueClass( IntWritable .class);
		int val=countjob.waitForCompletion( true)  ? 0 : 1;
		
		//Job conf for LinkGraph
		Configuration config=new Configuration();
		Job linkjob  = Job .getInstance(config, " link ");
		linkjob.setJarByClass( this .getClass());
		FileInputFormat.addInputPaths(linkjob,  args[0]);
		FileOutputFormat.setOutputPath(linkjob,  new Path(args[1]));
		linkjob.setMapperClass( LinkMap .class);
		linkjob.setReducerClass( LinkReduce .class);
		linkjob.setOutputKeyClass( Text .class);
		linkjob.setOutputValueClass( Text.class);
		//Store output as Sequence file.
		linkjob.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		return linkjob.waitForCompletion( true)  ? 0 : 1;
	}

	public static class CountMap extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable( 1);
		private Text currentWord  = new Text();

		public void map( LongWritable offset,  Text lineText,  Context context)
		throws  IOException,  InterruptedException {
			
			String line  = lineText.toString();
			//LineCount program similar to word count. Append one to each line in Map opertation.
			String fileName= ((FileSplit)context.getInputSplit()).getPath().getName();

			
			if (line.isEmpty()) {
				
			}
			else{
				currentWord  = new Text(new StringBuilder().append(fileName).toString());

				context.write(currentWord,one);
				
			}
		}
	}

	public static class CountReduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
		throws IOException,  InterruptedException {
			int sum  = 0;
			//Count lines.
			for ( IntWritable count  : counts) {
				sum  += count.get();
			}
			context.write(word,  new IntWritable(sum));
		}
	}
	

	
	public static class LinkMap extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		
		private double n=0.00;
		private Text mapKeyOutput  = new Text();

		public void map( LongWritable offset,  Text lineText,  Context context)
		throws  IOException,  InterruptedException {
				
			 //Location of file in HDFS
			Path p = new Path(lineCount_loc); 
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] files = fs.listStatus(p);
			for (FileStatus file : files) {
				//get output file of linecount
				if (file.getPath().getName().startsWith("part")) {
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
					String line_count;
					line_count = br.readLine();
					
					if (line_count != null && !"".equals(line_count)) {
						String[] parts=line_count.split("\\s+");
						//Calculate inital pagerank.
						n=1.00/Integer.parseInt(parts[1]);

					}
				}
			}
			
			String line  = lineText.toString();
			StringBuilder builder = new StringBuilder();
			//Enclose pagerank with pr tags. This makes easy to extract pagerank in future using regex.
			builder.append("<pr>").append(n).append("</pr>");
			if (line != null){
				//Extract title.
				Matcher matcher_title = TITLE_REGEX.matcher(line);
				if(matcher_title.find()){
					final List<String> linkValues = new ArrayList<String>();
					//Extract outlinks for the title.
					final Matcher matcher = LINK_REGEX.matcher(line);
					while (matcher.find()) {
						//Store all outlinks in a list.
						linkValues.add(matcher.group(1));
					}

					//Enclose outlinks with l tags. This makes easy to extract links in future using regex.
					for(String s : linkValues) {
						if (!"".equals(s.trim())) {
							builder.append("<l>").append(s.trim()).append("</l>");
							
						}
					}
					//Map Key: Title, Value: Initial pr and outlinks
					mapKeyOutput  = new Text(new StringBuilder().append(matcher_title.group(1)).toString());
					//  Format: "page   <pr>1/n</pr> <l>x</l> <l>y</l>....." where x ,y .. are outlinks for page.
					context.write(mapKeyOutput, new Text(builder.toString()));
				}
			}	
		}

	}

	public static class LinkReduce extends Reducer<Text ,  Text ,  Text , Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
		throws IOException,  InterruptedException {
			
			//Reduce operation is just identity funtion.
			for (Text count : counts) {
				context.write(word, count);
			}
			
		}
	}
}

