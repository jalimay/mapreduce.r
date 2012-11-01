package com.jd.ipc.mapreduce.r;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RMapperTest {
	RMapper map = new RMapper();

	@Before
	public void setup() throws IOException, InterruptedException {
		map.setup(null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void map() throws IOException, InterruptedException {
		LongWritable key = null;
		Text value = null;
		Context context = Mockito.mock(Context.class);
		map.map(key, value, context);
	}
}
