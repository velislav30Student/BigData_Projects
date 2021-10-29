package Zadanie1.Zadanie1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PaymentTypeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		//Skip Header Row - contains Column Names
		if(key.get() == 0) {
			return;
		}
		
		int paymentTypeColumnIndex = 3;
		
		String row = value.toString();
		
		StringTokenizer tokenizer = new StringTokenizer(row, ",");
		
		//Array is needed to get the token that corresponds to the needed column
		ArrayList<String> tokens = new ArrayList<String>();
		
		while (tokenizer.hasMoreTokens()) {
			tokens.add(tokenizer.nextToken().replace(" ", ""));
		}
		
		Text outputKey = new Text(tokens.get(paymentTypeColumnIndex));
		output.collect(outputKey, new IntWritable(1));
	}

}
