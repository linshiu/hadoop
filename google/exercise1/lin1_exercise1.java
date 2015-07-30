// default import not working, so import more specific

// import java io
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// import hadoop io fomrmat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

// import hadoop setup
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// import hadoop types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// import hadoop classes
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/*
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
*/

/*

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
*/

/*

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

*/

public class lin1_exercise1 {

	// custom class
	public static class Pair implements Writable {
		private int value1;
		private int value2;
		
		// initialize
		public Pair() {
			int value1 = 0;
			int value2= 0;
		}
		
		// set values
		public Pair(int value1, int value2) {
			this.value1 = value1;
			this.value2 = value2;
		}

		// getters
		public int getValue1()
		{
        	return value1;
   		}

   		public int getValue2()
		{
        	return value2;
   		}

   		// setters
    	public void setValue1(int value1) {
        	this.value1 = value1;
    	}

    	public void setValue2(int value2) {
        	this.value2 = value2;
    	}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			value1 = in.readInt( );
			value2 = in.readInt( );
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(value1);
			out.writeInt(value2);
		}
	}


	// map class for 1gram
    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, Pair> {

	    private Text yearSubstring= new Text();
	    private Pair pair= new Pair();
		
		// map function to process 1gram
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();

			// break different fields in the line by tab
			// or one or more white space "\\s+" since 2gram the two words are not separated by tab
		    String[] columns = line.split("\\s+");
		    //System.out.println(Arrays.toString(columns));
		   
		    // convert to integer
		    // http://stackoverflow.com/questions/8336607/how-to-check-if-the-value-is-integer-in-java
		    
		    try 
		    {	
		        Integer.parseInt(columns[1]);
		        
		        // convert lower case
		        
		        String word = columns[0].toLowerCase();

		        String [] listSubstrings = {"nu", "die", "kla"};
		        
		        for (String sub : listSubstrings)
		        {
		        	if (word.contains(sub))
			        {
			        	String index = columns[1] + " " + sub;
			        	//System.out.println(index + " " + columns[3]);	
			        	int volume = Integer.parseInt(columns[3]);
			        	yearSubstring.set(index);
			        	pair.setValue1(1);
			        	pair.setValue2(volume);
						context.write(yearSubstring,pair);
			        	
			        }
		        }	
		    
		    } // end try
		    		    
		    catch (NumberFormatException e)  { } //do nothing
		     

		} // end map function
    } // end map class

    // map class for 2gram
    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Pair> {

	    private Text yearSubstring= new Text();
	    private Pair pair= new Pair();
		
    	// map function to process 2gram
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();

			// break different fields in the line by tab
			// or one or more white space "\\s+" since 2gram the two words are not separated by tab
		    String[] columns = line.split("\\s+");
		    //System.out.println(Arrays.toString(columns));
		   
		    // convert to integer
		    // http://stackoverflow.com/questions/8336607/how-to-check-if-the-value-is-integer-in-java
		    
		    try 
		    {	
		        Integer.parseInt(columns[2]);
		        
		        // convert lower case
		        
		        String word1 = columns[0].toLowerCase();
		        String word2 = columns[1].toLowerCase();

		        String [] listSubstrings = {"nu", "die", "kla"};
		        
		        for (String sub : listSubstrings)
		        {
		        	if (word1.contains(sub) || word2.contains(sub))
			        {
			        	String index = columns[2] + " " + sub;
			        	//System.out.println(index + " " + columns[3]);	
			        	int volume = Integer.parseInt(columns[4]);
			        	yearSubstring.set(index);
			        	pair.setValue1(1);
			        	pair.setValue2(volume);
						context.write(yearSubstring,pair);	        	
			        }
		        }	
		    
		    } // end try
		    		    catch (NumberFormatException e)  { } //do nothing

		} // end map function
    } // end map class

    

    public static class MyCombiner extends Reducer <Text, Pair, Text, Pair> {

    	private Pair pair= new Pair();

		public void reduce( Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			
			int count = 0;
			int sum = 0;

			for (Pair value:values) {

				count += value.value1;
				sum += value.value2;
				
			}

			pair.setValue1(count);
			pair.setValue2(sum);
			context.write( key, pair );
		}
	}

	

    public static class MyReducer extends Reducer< Text, Pair, Text, DoubleWritable> {

    	private DoubleWritable avg= new DoubleWritable();
		public void reduce( Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {

			int count = 0;
			double sum= 0; // double since otherwise divide two integers get an integer
			
			for (Pair pair:values) 
			{
				count += pair.value1;
				sum += pair.value2;
				
			}

			avg.set(sum/count);
			
			context.write( key, avg);
		}
    }

	public static void main(String[] args) throws Exception {
		// Sources:
	    // Mutiple inputs: http://www.lichun.cc/blog/2012/05/hadoop-multipleinputs-usage/
	    // Custom classs: http://www.cs.bgu.ac.il/~dsp112/Forum?action=show-thread&id=5a15ede6df2520f2b68db15f2ce752fa

		Path firstPath = new Path(args[0]);
		Path sencondPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(lin1_exercise1.class);
		job.setJobName("avg books");

		//conf.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyCombiner.class);

	    //output format for mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);

	    //output format for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

	     //use MultipleOutputs and specify different Record class and Input formats
		MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(job, sencondPath, TextInputFormat.class, MyMapper2.class);
		//FileInputFormat.setInputPaths(conf, new Path(args[0]));

		//conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		//conf.setNumReduceTasks(5);
		//conf.setPartitionerClass(MusicPartitioner.class

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}		
}