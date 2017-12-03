package org.vikas;

import java.io.IOException;
import java.util.regex.Pattern;
import java.math.BigDecimal;
import java.util.regex.Pattern;
import java.util.regex.Matcher; 

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public class SortLinks extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(SortLinks.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new SortLinks(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "SortLinks");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setSortComparatorClass(Sorter.class);
		job.setNumReduceTasks(1);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {

		private Text word = new Text();
		private long numRecords = 0;    
	

		public void map(Text key, Text lineText, Context context)
		throws IOException, InterruptedException {
			String line = lineText.toString();
			if(!line.isEmpty()){
				
				final Pattern pattern_pr = Pattern.compile("<pr>(.+?)</pr>");
				
				Matcher matcher_pr= pattern_pr.matcher(line);
				matcher_pr.find();
				Double pr=Double.parseDouble(matcher_pr.group(1));
				//output <pagerank, title> pairs
				context.write(new Text(pr.toString()),key);
				
				
				
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text rank, Iterable<Text> links, Context context)
		throws IOException, InterruptedException {
				//Double prank=Double.parseDouble(rank.toString());
			for (Text link : links) {
				//output <title, pagerank> pairs.
				context.write(link, rank);
			}
		}
	}
	static class Sorter extends WritableComparator {

		protected Sorter() {
	      
	    }
		//Compare method from https://hadoop.apache.org/docs/r2.7.1/api/org/apache/hadoop/io/WritableComparator.html
	 public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				BigDecimal value1 = new BigDecimal(new String(b1, s1, l1).substring(1));
				BigDecimal value2 = new BigDecimal(new String(b2, s2, l2).substring(1));
				if (value1.compareTo(value2) > 0) {
					return -1;
				} else if (value1.compareTo(value2) < 0) {
					return 1;
				}
			} catch (NumberFormatException e) {
				throw new NumberFormatException("STRING THAT WAS BEING PARSED: " + new String(b1, s1, l1) + " AND " + new String(b2, s2, l2));
			}
			return 0;
		}
	}
	
}