package output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class DataRecordWriter<K, V>extends RecordWriter<K, V>{

	private DataOutputStream out;
	
	public DataRecordWriter(DataOutputStream out) {
		this.out = out;
	}
	
	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		((Writable)key).write(out);
		((Writable)value).write(out);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		this.out.flush();
		this.out.close();
	}
}
