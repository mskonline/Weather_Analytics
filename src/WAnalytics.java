/**
 * 
 * CS 5331 - Team 11
 * 
 * @author Bhushan Yavagal <bhushan.yavagal@mav.uta.edu>
 * 		   Sai Kumar Manakan <saikumar.manakan@mavs.uta.edu>
 * 
 */

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WAnalytics {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 3 Vector generation
		Configuration conf = new Configuration();

		// Setting split size (in Bytes):- Increases number of mappers based on
		// number of InputSplits
		//conf.set("mapred.max.split.size", "67108864"); // 64 MB
		Job job_3v = new Job(conf);

		job_3v.setJobName("Weather Data Analytics - 3 Vector Generation");
		job_3v.setJarByClass(WAnalytics.class);

		// Mapper outputs
		job_3v.setMapOutputKeyClass(Text.class);
		job_3v.setMapOutputValueClass(DoubleArrayWritable.class);

		// Reducer outputs
		job_3v.setOutputKeyClass(Text.class);
		job_3v.setOutputValueClass(Text.class);
		job_3v.setOutputFormatClass(TextOutputFormat.class);

		// Mapper
		job_3v.setMapperClass(W3V_Mapper.class);

		// set the combiner class for custom combiner
		job_3v.setCombinerClass(W3V_Combiner.class);

		// Reducer
		job_3v.setReducerClass(W3V_Reducer.class);

		// Setting the number of reducers
		// job_3v.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job_3v, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_3v, new Path(args[1] + "_3v"));

		job_3v.waitForCompletion(true);

		// 12 Vector generation
		Configuration conf2 = new Configuration();
		// Setting split size (in Bytes):- Increases number of mappers based on
		// number of InputSplits
		// conf2.set("mapred.max.split.size", "67108864"); // 64 MB
		Job job_12v = new Job(conf2);

		job_12v.setJobName("Weather Data Analytics  - 12 Vector Generation");
		job_12v.setJarByClass(WAnalytics.class);

		// Mapper outputs
		job_12v.setMapOutputKeyClass(Text.class);
		job_12v.setMapOutputValueClass(DoubleArrayWritable.class);

		// Reducer outputs
		job_12v.setOutputKeyClass(Text.class);
		job_12v.setOutputValueClass(Text.class);
		job_12v.setOutputFormatClass(TextOutputFormat.class);

		// Mapper
		job_12v.setMapperClass(W12V_Mapper.class);

		// Reducer
		job_12v.setReducerClass(W12V_Reducer.class);

		// Setting the number of reducers
		// job_12v.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job_12v, new Path(args[1] + "_3v"));
		FileOutputFormat.setOutputPath(job_12v, new Path(args[1]));

		job_12v.waitForCompletion(true);
		/*
		// Delete the temporary directory
		FileSystem hdfs = FileSystem.get(new Configuration());
		if (hdfs.exists(new Path(args[1] + "_3v"))) {
			hdfs.delete(new Path(args[1] + "_3v"), true);
		}*/
	}

	public static class W3V_Mapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException {
			
			if (key.get() != 0 /* Exclude reading the header */) {
				String stId,month,year,hr;
				String[] data;
				try {
					String str = value.toString();
					data = str.trim().replaceAll("\\s+", " ").split(" ");

					stId = data[0];
					month = data[2].substring(4, 6);
					year = data[2].substring(0, 4);
					hr = data[2].split("_")[1];
				} catch (Exception e) {
					return;
				}
				
				
				int int_hr = Integer.parseInt(hr);
				int section;

				if (int_hr >= 5 && int_hr <= 11)
					
					section = 1;
				else if (int_hr >= 11 && int_hr <= 17)
					section = 2;
				else if (int_hr >= 17 && int_hr <= 23)
					section = 3;
				else
					section = 4;

				String mkey = stId + "_" + year + "_" + month + "_" + section;

				Double tmp = Double.valueOf(data[3]);
				Double dp = Double.valueOf(data[4]);
				Double ws = Double.valueOf(data[12]);

				c.write(new Text(mkey), new DoubleArrayWritable(new Double[] { tmp, dp, ws }));
			}
		}
	}

	public static class W3V_Combiner extends Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {
		
		@Override
		public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context c)
				throws IOException, InterruptedException {
			double tCount = 0, dCount = 0, wsCount = 0;
			double t_sum = 0;
			double dp_sum = 0;
			double ws_sum = 0;

			for (ArrayWritable val : values) {
				Writable[] wA = val.get();

				DoubleWritable t = (DoubleWritable) wA[0];

				if (t.get() != 9999.9) {
					t_sum += t.get();
					++tCount;
				}

				t = (DoubleWritable) wA[1];

				if (t.get() != 9999.9) {
					dp_sum += t.get();
					++dCount;
				}

				t = (DoubleWritable) wA[2];

				if (t.get() != 999.9) {
					ws_sum += t.get();
					++wsCount;
				}
			}

			c.write(key, new DoubleArrayWritable(new Double[] { t_sum, dp_sum, ws_sum, tCount, dCount, wsCount }));
		}
	}

	// reducer
	public static class W3V_Reducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {

		DecimalFormat df = new DecimalFormat("#.00");

		@Override
		public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context c)
				throws IOException, InterruptedException {
			double tAvg = 0;
			double dpAvg = 0;
			double wsAvg = 0;

			for (ArrayWritable val : values) {
				Writable[] wA = val.get();

				double t_sum = 0;
				double dp_sum = 0;
				double ws_sum = 0;
				double tCount = 0, dCount = 0, wsCount = 0;

				t_sum = ((DoubleWritable) wA[0]).get();
				dp_sum = ((DoubleWritable) wA[1]).get();
				ws_sum = ((DoubleWritable) wA[2]).get();
				tCount = ((DoubleWritable) wA[3]).get();
				dCount = ((DoubleWritable) wA[4]).get();
				wsCount = ((DoubleWritable) wA[5]).get();

				// Calculating averages
				if(tCount != 0)
					tAvg = t_sum / tCount;
				
				if(dCount != 0)
					dpAvg = dp_sum / dCount;
				
				if(wsCount != 0)
					wsAvg = ws_sum / wsCount;
			}

			String keyData = key.toString().replaceAll("_", " ");
			key.set(keyData);

			c.write(key, new Text(df.format(tAvg) + " " + df.format(dpAvg) + " " + df.format(wsAvg)));
		}
	}

	// Mapper
	public static class W12V_Mapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable> {

		@Override
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException {

			String str = value.toString();

			// Removing all repeating spaces within the data and split by
			// single space
			String[] data = str.trim().replaceAll("\\s+", " ").split(" ");

			// Extracting data
			String stId = data[0];
			String year = data[1];
			String month = data[2];
			String section = data[3];

			// Key : StationId_Year_Month
			String mkey = stId + "_" + year + "_" + month;

			Double sec = Double.valueOf(section);
			Double tmp = Double.valueOf(data[4]); // Temperature
			Double dp = Double.valueOf(data[5]); // Dew Point
			Double ws = Double.valueOf(data[6]); // Wind Speed

			c.write(new Text(mkey), new DoubleArrayWritable(new Double[] { sec, tmp, dp, ws }));
		}
	}

	// Reducer
	public static class W12V_Reducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {

		DecimalFormat df = new DecimalFormat("#00.00");
		
		protected void setup(Context context) throws IOException, InterruptedException {
			// Writing the header in the output
			context.write(new Text("STN--- YEAR MN"), new Text(
					"S1_AT S1_AD S1_AW S2_AT S2_AD S2_AW S3_AT S3_AD S3_AW S4_AT S4_AD S4_AW"));
		}

		@Override
		public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context c)
				throws IOException, InterruptedException {

			// 12 Vector Attribute for each section
			double[] sAvgs = new double[12];

			StringBuffer output = new StringBuffer();

			for (ArrayWritable val : values) {
				Writable[] wA = val.get();

				DoubleWritable t = (DoubleWritable) wA[0];
				int sec = (int) t.get() - 1;

				t = (DoubleWritable) wA[1];
				sAvgs[3 * sec] = t.get();

				t = (DoubleWritable) wA[2];
				sAvgs[3 * sec + 1] = t.get();

				t = (DoubleWritable) wA[3];
				sAvgs[3 * sec + 2] = t.get();
			}

			for (int i = 0; i < 12; ++i) {
				output.append(df.format(sAvgs[i]) + " ");
			}

			String keyData = key.toString().replaceAll("_", " ");
			key.set(keyData);

			c.write(key, new Text(output.toString()));
		}
	}

	/**
	 * 
	 * This class extends ArrayWritable and sets array of Doubles Used in
	 * sending array of Doubles from Mapper to the Reducer
	 *
	 */
	public static class DoubleArrayWritable extends ArrayWritable {
		public DoubleArrayWritable() {
			super(DoubleWritable.class);
		}

		public DoubleArrayWritable(Double[] vals) {
			super(DoubleWritable.class);
			DoubleWritable[] dVals = new DoubleWritable[vals.length];

			for (int i = 0; i < vals.length; i++) {
				dVals[i] = new DoubleWritable(vals[i]);
			}
			set(dVals);
		}
	}
}
