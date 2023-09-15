package org.huang.flink.graph3;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.TernaryBoolean;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class Graph3 {
    public static final int PORT = 8081;
    public static final int DEFAULT_PARALLELISM = 1;
    public static final String STREAM_NAME = "simple stream without jar";

    private static final String SOURCE_MESSAGE = "============ hello flink, i like flink, flink is a even driven stream process framework";

    private static final Random RND = new Random();

    public static void main(String[] args) throws Exception {
        custom();
    }

    /**
     * 经典方式提交一个任务到服务器执行
     *
     * @throws Exception
     */
    private static void classic() throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", PORT);
        env.setParallelism(DEFAULT_PARALLELISM);

        DataStreamSource<String> source = env.fromElements(SOURCE_MESSAGE);
        source.print().disableChaining();
        env.execute(STREAM_NAME);
    }

    /**
     * 手工构造一个job graph并提交到远程服务器执行
     *
     * @throws Exception
     */
    private static void custom() throws Exception {
        final JobGraph jobGraph = getJobGraphInClassic();
        //final JobGraph jobGraph = getJobGraphInCustom();

        byte[] jobGraphBytes = serializeJobGraph(jobGraph);

        final String jobGraphFileName = getFileName();
        final Collection<String> jarFileNames = Collections.emptyList();
        final Collection<JobSubmitRequestBody.DistributedCacheFile> artifactFileNames = Collections.emptyList();
        JobSubmitRequestBody body = new JobSubmitRequestBody(jobGraphFileName, jarFileNames, artifactFileNames);

        sendHttpRequest(body,jobGraphBytes,Collections.emptyList());
        //env.execute("simple stream without jar");
    }

    /**
     * 使用fnlik api生成job graph
     * @return
     */
    private static JobGraph getJobGraphInClassic() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataStreamSource<String> source = env.fromElements(SOURCE_MESSAGE);
        source.print().disableChaining();

        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobName(STREAM_NAME);
        return streamGraph.getJobGraph();
    }

    /**
     * 手工生成job graph
     * @return
     */
    private static JobGraph getJobGraphInCustom() throws IOException {
        //FIXME 这里修改问完全手工构建 JobGraph 删除StreamExecutionEnvironment和streamGraph相关代码

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataStreamSource<String> source = env.fromElements(SOURCE_MESSAGE);
        source.print().disableChaining();

        JobGraph jobGraph = new JobGraph(STREAM_NAME);
        jobGraph.setJobType(JobType.STREAMING);
        jobGraph.setDynamic(false);
        jobGraph.enableApproximateLocalRecovery(false);
        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.none());

        final Configuration configuration = new Configuration();
        //TODO 这里设置配置
        final ExecutionConfig config = new ExecutionConfig();
        config.configure(configuration, Graph3.class.getClassLoader());
        jobGraph.setExecutionConfig(config);

        jobGraph.setChangelogStateBackendEnabled(TernaryBoolean.UNDEFINED);

        List<OperatorIDPair> operatorIDPairs = new ArrayList<>();

        JobVertexID jobVertexId = new JobVertexID(RND.nextLong(),RND.nextLong());
        final JobVertex sinkVertex = new JobVertex("Sink: Print to Std. Out", jobVertexId, operatorIDPairs);

        //final JobVertex sourceVertex = new JobVertex("Source: Collection Source", jobVertexId, operatorIDPairs);

        return jobGraph;
    }

    private static byte[] serializeJobGraph(JobGraph jobGraph) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(jobGraph);
        oos.flush();
        return baos.toByteArray();
    }

    private static void sendHttpRequest(final JobSubmitRequestBody body, byte[] bodyGraphObjSerialize, List<byte[]> jars) throws Exception {
        String bodyJsonStr = RestMapperUtils.getStrictObjectMapper().writeValueAsString(body);
        URL url = new URL("http://127.0.0.1:" + PORT + "/v1/jobs");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setConnectTimeout(15000);
        connection.setReadTimeout(60000);
        final String boundary = "b34832b4894dde24";
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
        connection.setRequestProperty("transfer-encoding", "chunked");
        connection.setRequestProperty("connection", "close");
        connection.connect();

        DataOutputStream out = new DataOutputStream(connection.getOutputStream());
        writeUtf8(out, "1fa0\r\n--");
        writeUtf8(out, boundary);
        writeUtf8(out, "\r\n");
        writeUtf8(out, "content-disposition: form-data; name=\"request\"\r\n");
        writeUtf8(out, "content-length: ");
        writeUtf8(out, String.valueOf(bodyJsonStr.length()));
        writeUtf8(out, "\r\n");
        writeUtf8(out, "content-type: text/plain; charset=UTF-8\r\n");
        writeUtf8(out, "\r\n");
        writeUtf8(out, bodyJsonStr);
        writeUtf8(out, "\r\n");


        writeUtf8(out, "--");
        writeUtf8(out, boundary);
        writeUtf8(out, "\r\n");
        writeUtf8(out, "content-disposition: form-data; name=\"file_0\"; filename=\"");
        writeUtf8(out, body.jobGraphFileName);
        writeUtf8(out, "\"\r\n");
        writeUtf8(out, "content-length: ");
        writeUtf8(out, String.valueOf(bodyGraphObjSerialize.length));
        writeUtf8(out, "\r\n");
        writeUtf8(out, "content-type: application/octet-stream\r\n");
        writeUtf8(out, "content-transfer-encoding: binary\r\n");
        writeUtf8(out, "\r\n");
        out.write(bodyGraphObjSerialize);

        writeUtf8(out, "\r\n");
        writeUtf8(out, "--");
        writeUtf8(out, boundary);
        writeUtf8(out, "--\r\n");

        out.flush();
        out.close();

        BufferedReader bf = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
        String line = null;
        while ((line=bf.readLine())!=null) {
            System.out.println(line);
        }
        connection.disconnect();
    }

    private static void writeUtf8(DataOutputStream out, String content) throws IOException {
        out.write(content.getBytes(StandardCharsets.UTF_8));
    }

    private static String getFileName() throws IOException {
        final SecureRandom random = new SecureRandom();
        return "flink-jobgraph" + Long.toUnsignedString(random.nextLong()) + ".bin";
    }
}
