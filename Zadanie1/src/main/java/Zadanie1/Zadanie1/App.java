package Zadanie1.Zadanie1;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

public class App 
{
	
	enum JobType{
		PaymentType,
		Product,
		Day
	}
	
    public static void main( String[] args ) throws IOException
    {
    	runJob(JobType.PaymentType);
    	runJob(JobType.Product);
    	runJob(JobType.Day);
    }
    
    private static void runJob(JobType jobType) throws IOException {
    	
    	Configuration conf = new Configuration();
    	
    	Path inputPath = new Path("hdfs://127.0.0.1:9000/input/SalesJan2009.csv");
    	Path outputPath = getOutputPath(jobType);

    	Class<? extends Mapper<?,?,?,?>> mapperClass = getMapperClass(jobType);
    	
        JobConf job = new JobConf(conf, App.class);
        
        job.setJobName("NumOfAllPaymentsByType");
        job.setJarByClass(App.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(CountReducer.class);
        
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
        
        if(hdfs.exists(outputPath)) {
     	   hdfs.delete(outputPath, true);
        }
        
        RunningJob result = JobClient.runJob(job);
        
        System.out.println("Success = " + result.isComplete());
    }
    
    private static Path getOutputPath(JobType jobType) {
    	switch (jobType) {
		case PaymentType:
			return new Path("hdfs://127.0.0.1:9000/output/NumOfAllPaymentsByType.txt");
		case Product:
			return new Path("hdfs://127.0.0.1:9000/output/NumOfAllPaymentsByProduct.txt");
		case Day:
			return new Path("hdfs://127.0.0.1:9000/output/NumOfAllPaymentsByDay.txt");
		}
		return null;
    }
    
    private static Class<? extends Mapper<?, ?, ?, ?>> getMapperClass(JobType jobType){
    	switch (jobType) {
		case PaymentType:
			return PaymentTypeMapper.class;
		case Product:
			 return ProductMapper.class;
		case Day:
			return DayMapper.class;
		}
		return null;
    }
}
