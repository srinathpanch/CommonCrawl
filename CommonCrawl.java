package org.commoncrawl.examples.mapreduce;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.warc.WARCFileInputFormat;

/**
 * Gets URLs and Server names from the common crawl dataset.
 *
 * @author Stephen Merity (Smerity)
 */
public class CommonCrawl extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CommonCrawl.class);
	
	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the Hadoop job. 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CommonCrawl(), args);
		System.exit(res);
	}

	/**
	 * Builds and runs the Hadoop job.
	 * @return	0 if the Hadoop job completes successfully and 1 otherwise.
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		//
		Job job = new Job(conf);
		job.setJarByClass(CommonCrawl.class);
		
		LOG.info("Common Crawl program");

		// ----- INPUT -----
		// Option 1: Look at a single WARC file.
		//String inputPath = "s3://commoncrawl/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/*.warc.gz";
		String inputPath = "s3://commoncrawl/crawl-data/CC-MAIN-2018-13/segments/*/warc/*.warc.gz";

		// Option 2: Look at a whole segment of WARC files
		//  May take many minutes just to read the file listing.
		// String inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/warc/*.warc.gz";
		LOG.info("Input path: '" + inputPath + "'");
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// ------ OUTPUT ----
		// Set the path where final output 'part' files will be saved.
		// (must be set as an argument when running this class)
		String outputPath = arg0[0];

		// Delete the output path directory if it already exists.
		LOG.info("Clearing the output path at '" + outputPath + "'");
		FileSystem fs = FileSystem.get(new URI(outputPath), conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}

		// Set the path where final output 'part' files will be saved.
		LOG.info("Output path: '" + outputPath + "'");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);
		
		//for WARC type files 
		job.setInputFormatClass(WARCFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	    
		job.setMapperClass(CommonCrawlMap.CommonCrawlMapper.class);
		job.setReducerClass(LongSumReducer.class);

		// ----- Settings -----
		// Uncomment this line to set a single reducer
		// Pro: You will get 1 output file, which might be nicer
		// Con: You will have a performance bottleneck in your cluster
		//   because only 1 node will be performing the reduce task
		//
		// job.setNumReduceTasks(1);
		
		// Pacific customization:
		// Allow a small percent of the map tasks to fail without failing the
		// entire job. (Rationale: If we're processing CommonCrawl, who cares if
		// a few ARC files get missed?)
		conf.set("mapreduce.map.failures.maxpercent", "5");
		//
		// Tip: If your job is 99% done and a few map tasks are still slowly
		// processing, you can kill them manually:
		// 1.) SSH to the master node
		// 2.) Run lynx and browse the Hadoop web interface
		// 3.) Note the attempt IDs of the still-running map tasks
		// They should look like this: attempt_201210111830_0012_m_000000_0
		// 4.) Back at the command-line on the master node, kill each map task:
		// hadoop job -kill-tasks <attempt ID>
		// (Or, perhaps "-fail-tasks" is appropriate)

		// Pacific customization:
		// Don't bother retrying failed map tasks more than once
		// (Rationale: If a map task fails, it is 99.99% likely that the cause
		// is a bug in student code that only happens on some rare input data.
		// The bug might be in a library, like Apache Tika or JSoup, but it is
		// still *your* fault for not setting a timeout in your map function and
		// just skipping that file after some reasonable wait. In any case,
		// retrying the same input data will just result in the same crash
		// at the same place, so why bother? Node failures in EC2 are infrequent
		// on the short timescale of these student projects.)
		conf.set("mapreduce.map.maxattempts", "1");
	    
		// Submit the job, then poll for progress until the job is complete
		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}
}
