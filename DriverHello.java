package hello.world;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DriverHello {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Use programArgs array to retrieve program arguments.
		String[] programArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if(programArgs[2] != null)
			conf.setStrings("ProductId", programArgs[2]);
		if(programArgs[2] != null)
			conf.setStrings("CountryName", programArgs[3]);
		if(programArgs[2] != null)
		conf.setStrings("SearchDate",programArgs[4]);
		
		Job job = new Job(conf,"Contoso");
		job.setJarByClass(DriverHello.class);
		job.setMapperClass(HelloMapper.class);
		job.setReducerClass(ReducerHello.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: Update the input path for the location of the inputs of the map-reduce job.
		FileInputFormat.addInputPath(job, new Path(programArgs[0]));
		// TODO: Update the output path for the output directory of the map-reduce job.
		FileOutputFormat.setOutputPath(job, new Path(programArgs[1]));

		// Submit the job and wait for it to finish.
		job.waitForCompletion(true);
		// Submit and return immediately: 
		// job.submit();
	}

}
