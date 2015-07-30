import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class lin1_exercise4 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text artist = new Text();
    private DoubleWritable duration = new DoubleWritable();
	

	public void configure(JobConf job) {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();

	    // break different fields in the line by comma only if that comma 
		// has zero, or an even number of quotes ahead of it
	    String[] columns = line.split(",");
	    
	    // Note: this avoids splitting the string like "Chicago, IL"
	    //http://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
	    /*
                ",                         "+ // match a comma
                "(?=                       "+ // start positive look ahead
                "  (                       "+ //   start group 1
                "    %s*                   "+ //     match 'otherThanQuote' zero or more times
                "    %s                    "+ //     match 'quotedString'
                "  )*                      "+ //   end group 1 and repeat it zero or more times
                "  %s*                     "+ //   match 'otherThanQuote'
                "  $                       "+ // match the end of the string
                ")                         ", // stop positive look ahead
	    */
		
		String artistS = columns[2];
		Double durationD = Double.parseDouble(columns[3] );
	
		artist.set(artistS);
		duration.set(durationD);

		output.collect(artist, duration);

	}
    }


    public static class Reduce extends MapReduceBase implements Reducer< Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce( Text key, Iterator<DoubleWritable> values, OutputCollector< Text, DoubleWritable> output, Reporter reporter) throws IOException {

	    double max = -9999;
	   
	    // iterating over values of the same key
	    // for every key, reducer is going to be executed 

	    while (values.hasNext()) {
			double current = values.next().get();
			if (current>max){max=current;}
	    }

	    output.collect(key, new DoubleWritable(max));
	}
    }

    public static class MusicPartitioner extends MapReduceBase implements Partitioner<Text, DoubleWritable> {
 
        
        public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {

        	String artist = key.toString();
    	    char first = artist.charAt(0);
    	    first = Character.toUpperCase(first);
 
            //if the age is <20, assign partition 0
            if(first <='E'){               
                return 0;
            }
       
            if(first <= 'J'){
            	return 1;
            }

            if(first <= 'O'){
            	return 2;
            }

            if(first <= 'T'){
            	return 3;
            }
        
            else{
                return 4 ;
            }
           
        }
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), lin1_exercise4.class);
	conf.setJobName("maxduration");

	conf.setNumReduceTasks(5);
	conf.setPartitionerClass(MusicPartitioner.class);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new lin1_exercise4(), args);
	System.exit(res);
    }
}