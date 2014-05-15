package edu.khush.lsi.pagerank.blocked;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.lsi.project2.pagerank.simplemr.PageRankSimple.Counters;

public class PageRankJob {

	public static final int precision = 10000;
	public static HashMap<Long, Long> nodeBlockIdMap = new HashMap<Long, Long>();
	public static BufferedWriter logFile = null;
	
	// hadoop counter to track the total residual error
	public static enum Counters {
		RESIDUAL_ERROR,NO_OF_ITERS
	};

	public static void main(String[] args) throws Exception {
		

		System.out.println("Size of map: " + nodeBlockIdMap.size());
		String input = "";
		String inputFilePath = "";
		String outputFilePath = "";
		String logFilePath = ".";

		System.out
				.println("Enter the filename and the output bucket seaparated by a space");

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try {
			input = br.readLine();
			String[] list = input.split(" ");
			inputFilePath = list[0];
			outputFilePath = list[1];
		} catch (IOException io) {
			System.out.println("Enter the correct values");
		}

		try {

			// Create log
			logFile = new BufferedWriter(new FileWriter("log_"
					+ System.currentTimeMillis(), true));
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		//Uncomment this and use this only when running for random partitioning input
		//readBlockIdMap(inputFilePath);

		int numRepititions = 7;

		for (int i = 0; i < numRepititions; i++) {

			logFile.write("Starting Iteration " + i);
			logFile.newLine();

			logFile.write("*************************");
			logFile.newLine();

			Job job = getBlockedPRJob(i, inputFilePath, outputFilePath);

			job.waitForCompletion(true); // run the job

			// computing average Residual Error and Avg number of iterations in
			// block page rank reducer. Use the Hadoop counter for getting
			// values
			double averageResidualError = job.getCounters()
					.findCounter(Counters.RESIDUAL_ERROR).getValue();
			double avgNoOfIters = (job.getCounters().findCounter(
					Counters.NO_OF_ITERS).getValue())
					/ new Double(68);
			averageResidualError = (averageResidualError / precision)
					/ new Double(68);

			String residualErrorMessage = String.format("%.5f",
					averageResidualError);
			System.out.println("Iteration " + i + " Residual Error : "
					+ residualErrorMessage);

			logFile.write("Iteration " + i + " Residual Error : "
					+ residualErrorMessage);
			logFile.newLine();
			logFile.write("Iteration " + i + " Avg no of iterations : "
					+ avgNoOfIters);
			logFile.newLine();
			job.getCounters().findCounter(Counters.RESIDUAL_ERROR).setValue(0L);
			job.getCounters().findCounter(Counters.NO_OF_ITERS).setValue(0L);

		}

		logFile.close();

	}

	public static Job getBlockedPRJob(int i, String ipPath, String opPath)
			throws IOException {

		Job job = new Job();

		// Set name
		job.setJobName("BlockedMR_PageRank_" + (i + 1));
		job.setJarByClass(edu.khush.lsi.pagerank.blocked.PageRankJob.class);

		// Set Mapper and Reducer class
		job.setMapperClass(edu.khush.lsi.pagerank.blocked.BlockPRMapper.class);
		job.setReducerClass(edu.khush.lsi.pagerank.blocked.BlockPRReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NodeOrEdge.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Node.class);

		job.setInputFormatClass(NodeInputFormat.class);
		job.setOutputFormatClass(NodeOutputFormat.class);

		if (i == 0) {
			FileInputFormat
					.addInputPath(
							job,
							new Path(
									ipPath));
		} else {
			// set the output of the previous pass as the input to next pass
			// "/Users/khushboopandia/CS5300/project2/io/output/"
			FileInputFormat.addInputPath(job, new Path(
					opPath + i));
		}

		FileOutputFormat.setOutputPath(job, new Path(
				opPath + (i + 1)));

		return job;
	}

	
	
	public static void readBlockIdMap(String ipPath) throws Exception {

		BufferedReader reader = new BufferedReader(new FileReader(
				ipPath));
		String line = "";
		Long nodeId = null;
		Long blockId = null;

		while ((line = reader.readLine()) != null) {
			nodeId = Long.parseLong(line.split(" ")[0]);
			blockId = Long.parseLong(line.split(" ")[1]);
			nodeBlockIdMap.put(nodeId, blockId);
		}

	}

}
