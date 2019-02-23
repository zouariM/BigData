package io.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import io.writable.impl.PointWritable;

public class PointInputFormat extends FileInputFormat<LongWritable, PointWritable>{

	@Override
	public RecordReader<LongWritable, PointWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new PointRecordReader();		
	}
	
}
