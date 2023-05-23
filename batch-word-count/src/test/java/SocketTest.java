import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketTest {

    @Test
    public void testListen() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 9000), 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
