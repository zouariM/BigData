package io.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import io.writable.impl.LabelPointWritable;

public class LabelPointInputFormat extends FileInputFormat<LongWritable, LabelPointWritable>{

	@Override
	public org.apache.hadoop.mapreduce.RecordReader<LongWritable, LabelPointWritable> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new LabelPointRecordReader();
	}

	
}
