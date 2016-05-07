import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class weatherAnalysis {
	// First Mapper Class
	// Input : Input File
	// Output key : StationId+Month+Day+Section, Output value
	// :Temperature+Wind+Dew
	public static class MyMap1 extends Mapper<LongWritable, Text, Text, Text> {
		int sec;

		public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
			if (k.get() != 0) {
				String data = v.toString();
				String arr[] = data.replaceAll("\\s+", " ").split(" ");
				long stid = Long.parseLong(arr[0]);
				int year = Integer.parseInt(arr[2].substring(0, 4));
				int month = Integer.parseInt(arr[2].substring(4, 6));
				int day = Integer.parseInt(arr[2].substring(6, 8));
				int hr = Integer.parseInt(arr[2].substring(9, 11));
				if (hr == 5 || hr == 6 || hr == 7 || hr == 8 || hr == 9 || hr == 10)
					sec = 1;
				else if (hr == 11 || hr == 12 || hr == 13 || hr == 14 || hr == 15 || hr == 16)
					sec = 2;
				else if (hr == 17 || hr == 18 || hr == 19 || hr == 20 || hr == 21 || hr == 22)
					sec = 3;
				else
					sec = 4;
				float temp = Float.parseFloat(arr[3]);
				float dew = Float.parseFloat(arr[4]);
				float wind = Float.parseFloat(arr[12]);
				c.write(new Text(stid + "," + month + "," + day + "," + sec), new Text(temp + "," + wind + "," + dew));

			}
		}

	}

	// First reducer class
	// Input Key : StatationID+Month+Day+Section , Input value:
	// Temperature+Wind+Dew
	// Output key : StationID+Month+Day+Section, Output
	// value:AvgTemp+AvgWind+AvgDew
	public static class MyRed1 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text k, Iterable<Text> v, Context c) throws IOException, InterruptedException {

			int tempCount = 0;
			int windCount = 0;
			int dewCount = 0;
			double sumTemp = 0.0;
			double sumWind = 0.0;
			double sumDew = 0.0;
			double tempMissing = 9999.9;
			double windMissing = 9999.9;
			double dewMissing = 9999.9;
			for (Text val : v) {

				String tempWindDew[] = val.toString().split(",");
				double temp = Double.parseDouble(tempWindDew[0].toString());
				double wind = Double.parseDouble(tempWindDew[1].toString());
				double dew = Double.parseDouble(tempWindDew[2].toString());
				if (temp != tempMissing) {
					sumTemp += temp;
					tempCount++;
				}
				if (wind != windMissing) {
					sumWind += wind;
					windCount++;
				}
				if (dew != dewMissing) {
					sumDew += dew;
					dewCount++;
				}
			}
			c.write(k,
					new Text(String.format("%.2f", (sumTemp / tempCount)) + ","
							+ String.format("%.2f", (sumWind / windCount)) + ","
							+ String.format("%.2f", (sumDew / dewCount))));

		}
	}

	// Second Mapper
	// Input key : StationID+Month+Day+Section, Input value:
	// AvgTemp+AvgWind+AvgDew
	// Output key : StationID+Month , Output value :
	// Section+AvgTemp+AvgWind=AvgDew
	public static class MyMap2 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {

			String data = v.toString();
			String arr[] = data.split(",");
			String keyToRed = arr[0] + arr[1];
			double avgTemp = Double.parseDouble(arr[4].toString());
			double avgWind = Double.parseDouble(arr[5].toString());
			double avgDew = Double.parseDouble(arr[6].toString());
			int sec = Integer.parseInt(arr[3].toString());
			c.write(new Text(keyToRed), new Text(sec + "," + avgTemp + "," + avgWind + "," + avgDew));

		}
	}

	// Second reducer class
	// Input key : StationID+Month, Input value: Section+AvgTemp+AvgWind+AvgDew
	// Output : Final Output file
	public static class MyRed2 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text k, Iterable<Text> v, Context c) throws IOException, InterruptedException {
			int tempCount1 = 0;
			int tempCount2 = 0;
			int tempCount3 = 0;
			int tempCount4 = 0;
			double sumTemp1 = 0.0;
			double sumTemp2 = 0.0;
			double sumTemp3 = 0.0;
			double sumTemp4 = 0.0;
			int windCount1 = 0;
			int windCount2 = 0;
			int windCount3 = 0;
			int windCount4 = 0;
			double sumWind1 = 0.0;
			double sumWind2 = 0.0;
			double sumWind3 = 0.0;
			double sumWind4 = 0.0;
			double sumDew1 = 0;
			double sumDew2 = 0;
			double sumDew3 = 0;
			double sumDew4 = 0;
			int dewCount1 = 0;
			int dewCount2 = 0;
			int dewCount3 = 0;
			int dewCount4 = 0;
			for (Text val : v) {

				String tempdata[] = val.toString().split(",");
				double temp = Double.parseDouble(tempdata[1].toString());
				double wind = Double.parseDouble(tempdata[2].toString());
				double dew = Double.parseDouble(tempdata[3].toString());
				int sec = Integer.parseInt(tempdata[0].toString());
				if (sec == 1) {
					sumTemp1 += temp;
					tempCount1++;
					sumWind1 += wind;
					windCount1++;
					sumDew1 += dew;
					dewCount1++;

				} else if (sec == 2) {
					sumTemp2 += temp;
					tempCount2++;
					sumWind2 += wind;
					windCount2++;
					sumDew2 += dew;
					dewCount2++;
				} else if (sec == 3) {
					sumTemp3 += temp;
					tempCount3++;
					sumWind3 += wind;
					windCount3++;
					sumDew3++;
					dewCount3++;
				} else {
					sumTemp4 += temp;
					tempCount4++;
					sumWind4 += wind;
					windCount4++;
					sumDew4 += dew;
					sumDew4++;
				}
			}
			c.write(k,
					new Text(String.format("%.2f", (sumTemp1 / tempCount1)) + " | "
							+ String.format("%.2f", (sumWind1 / windCount1)) + " | "
							+ String.format("%.2f", (sumDew1 / dewCount1)) + " | "
							+ String.format("%.2f", (sumTemp2 / tempCount2)) + " | "
							+ String.format("%.2f", (sumWind2 / windCount2)) + " | "
							+ String.format("%.2f", (sumDew2 / dewCount2)) + " | "
							+ String.format("%.2f", (sumTemp3 / tempCount3)) + " | "
							+ String.format("%.2f", (sumWind3 / windCount3)) + " | "
							+ String.format("%.2f", (sumDew3 / dewCount3)) + " | "
							+ String.format("%.2f", (sumTemp4 / tempCount4)) + " | "
							+ String.format("%.2f", (sumWind4 / windCount4)) + " | "
							+ String.format("%.2f", (sumDew4 / dewCount4))));
		}

	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "firstjob");
		// job1.setNumMapTasks(0);
		// job1.setNumReduceTasks(0);
		job1.setJarByClass(weatherAnalysis.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(MyMap1.class);
		job1.setReducerClass(MyRed1.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "secondJob");
		// job2.setNumMapTasks(2);
		// job2.setNumReduceTasks(2);
		job2.setJarByClass(weatherAnalysis.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(MyMap2.class);
		job2.setReducerClass(MyRed2.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.waitForCompletion(true);

	}

}
