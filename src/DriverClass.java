package org.vikas;

import org.apache.hadoop.conf.Configuration;    // Used for Configutration of system parameters.
import org.apache.hadoop.util.ToolRunner; 		  // Used to implement Tool interface
import org.apache.hadoop.fs.FileSystem; 
import org.vikas.LinkGraph;							  // Include LinkGraph Prgram
import org.vikas.PageRank;								  // Include PageRank Program
import org.vikas.SortLinks;									  // Include Sort Program



public class DriverClass{
	
	public static void main(String[] args) throws Exception {
		
		String inputPath=args[0];   //Input file 
		String outputPath=args[1];  //Output file
		String outputPathLinkGraph=outputPath+"/LinkGraph";
	
		int res_lg=-1;
		// Call LinkGraph job to generate inital pageRank and list of outlinks for each url in wiki-xml-file.
		res_lg= ToolRunner.run(new LinkGraph(),new String[] {inputPath,outputPathLinkGraph});	
		
		//Iterate PageRank for 10 Times to achieve Convergence.
		int numRuns=10;
		int res_pr=0;
		//After First iteration, pass outputPath of previous iteration has inputPath to the next iteration.
		String inputPathPageRank=outputPathLinkGraph;
		String outputPathPageRank=outputPath+"/PageRank";
	
		for(int i=1;i<=numRuns;i++){
			if(i!=1){
				inputPathPageRank=outputPathPageRank;
			}
			outputPathPageRank=outputPath+"/PageRank" +i;
			// Call PageRank job's 10 times to generate final pageRank for each url in wiki-xml-file.
			res_pr = ToolRunner.run(new PageRank(),new String[] {inputPathPageRank,outputPathPageRank});
		}
		
		//Delete temparory directories.
		for(int i=1;i<numRuns;i++){
			String path=outputPath+"/PageRank" +i;
		FileSystem fs = FileSystem.get(getConf());
		 if (fs.exists(new Path(path))) {
            fs.delete(new Path(path), true);
        }
		}
		//Sorting the pages based on the page rank value
		String outputPathSorted=outputPath+"/Sorted";
		int pageRank = ToolRunner.run(new SortLinks(), new String[] {outputPathPageRank,outputPathSorted});
		
		System.out.println("Final Sorted PageRank is stored in "+outputPathSorted);
	}
}