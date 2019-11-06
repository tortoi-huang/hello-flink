package org.huang.flink.hello;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class SokectSource {
	public static void main(String[] args) {
		try {
			new SkServer(9000).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static class SkServer extends Thread {
		private boolean running = true;
		final ServerSocketChannel server;
		final Selector selector;

		public SkServer(int port) throws IOException {
			server = ServerSocketChannel.open();
			server.configureBlocking(false);
			server.socket().bind(new InetSocketAddress(port),150);
			selector = Selector.open();
			server.register(selector, SelectionKey.OP_ACCEPT);
		}

		@Override
		public void run() {
			while(running) {
				try {
					selector.select();
					final Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
					while(iterator.hasNext()) {
						final SelectionKey next = iterator.next();
						iterator.remove();
						if(next.isReadable()) {
							readText(next);
						} else if(next.isAcceptable()) {
							SocketChannel socketChannel = ((ServerSocketChannel)next.channel()).accept();
							socketChannel.configureBlocking(false);
							socketChannel.register(next.selector(),SelectionKey.OP_READ);
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		private void readText(final SelectionKey next) throws IOException {
			ByteBuffer readBuff = ByteBuffer.allocate(1024);
			final SocketChannel channel = (SocketChannel)next.channel();
			while(channel.read(readBuff) > 0) {
				readBuff.flip();
				System.out.println("read:" + new String(readBuff.array()));
				readBuff.clear();
			}
			channel.close();
		}
	}

}
