package org.huang.flink.entrypoint;

import org.junit.Test;

import java.io.File;

public class JavaApiTest {

    @Test
    public void testFileBasePath() {
        File f = new File("./");
        String absolutePath = f.getAbsolutePath();
        System.out.println(absolutePath);
    }
}
