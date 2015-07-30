import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class lin1_exercise3 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private Text result = new Text("");
	private Text index = new Text();

	 // create a list with ranges of years to filter
	//private static ArrayList<Integer> yearsList = new ArrayList<Integer>();

	private static Set yearsList = new HashSet();
	//private static String N;
	public void configure(JobConf job) {
		 //N = job.get("test");
		
		for (int i = 2000; i<= 2010; i++)
		{
			yearsList.add(i);
			
		}


	}


	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    
	
		// read line
	    String line = value.toString();
		
		// break different fields in the line by comma only if that comma 
		// has zero, or an even number of quotes ahead of it
	    String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	    
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
	    
	    int year = Integer.parseInt(columns[165]);
 	    
 	    if (yearsList.contains(year))
 	    
 	    {
 	    	// artist name (field 2), duration (field 3), song title (field 1)
 		 	String indexS = columns[2] + " , " +  columns[3] + " , " + columns[1] ;

 		 	index.set(indexS);
 		 	//result.set(N);
 	    	
 		 	output.collect(index, result);    	
 	    }	    	
	    
		
		
	} // end map function
    } // end Map class

    /*
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    
	  
	}
    }
    */

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), lin1_exercise3.class);
	conf.setJobName("lin1_exercise3_job");
	//conf.set("test", "123");

	// no reducer
	conf.setNumReduceTasks(0);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(Map.class);
	// conf.setCombinerClass(Reduce.class);
	// conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new lin1_exercise3(), args);
	System.exit(res);
    }
}
