package Zadanie1.Zadanie1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DayMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		//Skip Header Row - contains Column Names
		if(key.get() == 0) {
			return;
		}
		
		String row = value.toString();
		
		StringTokenizer tokenizer = new StringTokenizer(row, ",");
		
		String date = tokenizer.nextToken();
		
		int whiteSpaceIndex = date.indexOf(" ");
		
		//Remove hours and minutes
		date = date.substring(0, whiteSpaceIndex);
		
		//Make all dates the same format
		date = date.replace("/", ".");
		
		//Make all month indicator/number the same
		if(date.startsWith("01")) {
			date = date.substring(1);
		}
		
		//Make all year indicator/number the same
		if(date.contains("2009")) {
			date = date.replace("2009", "09");
		}
		
		Text outputKey = new Text(date);
		output.collect(outputKey, new IntWritable(1));
	}

}