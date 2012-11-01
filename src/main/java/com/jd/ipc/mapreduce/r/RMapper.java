package com.jd.ipc.mapreduce.r;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;

import com.jd.ipc.mapreduce.r.util.RConnectionUtil;

public class RMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	private Logger log = Logger.getLogger(getClass().getName());

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		RConnection c = null;
		try {
			c = RConnectionUtil.getRConnection();
			double[] i = { 1, 2, 4, 0, 1, 3, 1, 11, 1, 12, 1, 7, 1, 9, 17 };
			c.assign("xw", i);
			c.eval(" fit<-arima(xw,order=c(1,1,1)) ");
			c.eval("lh.pred<- predict(fit,n.ahead=7)");
			@SuppressWarnings("unused")
			RList x = c.eval("lh.pred[1]").asList();
		} catch (Exception e) {
			log.error(e);
			throw new RuntimeException(e);
		} finally {
			RConnectionUtil.returnRConnection(c);
		}
	}
}
