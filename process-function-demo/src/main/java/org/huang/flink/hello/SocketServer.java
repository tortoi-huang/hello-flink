package org.huang.flink.hello;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.CharsetUtil;

import java.net.SocketAddress;

public class SocketServer {
    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;

    public void startServer(int port) {
        //创建两个线程组 boosGroup、workerGroup
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try {
            //创建服务端的启动对象，设置参数
            ServerBootstrap bootstrap = new ServerBootstrap();
            //设置两个线程组boosGroup和workerGroup
            bootstrap.group(bossGroup, workerGroup)
                    //设置服务端通道实现类型
                    .channel(NioServerSocketChannel.class)
                    //设置线程队列得到连接个数
                    .option(ChannelOption.SO_BACKLOG, 128)
                    //设置保持活动连接状态
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    //使用匿名内部类的形式初始化通道对象
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            //给pipeline管道设置处理器
                            socketChannel.pipeline().addLast(new MyServerHandler()).addLast(new MyBizHandler());
                        }
                    });//给workerGroup的EventLoop对应的管道设置处理器
            System.out.println("--netty server is ready now...");
            //绑定端口号，启动服务端
            ChannelFuture channelFuture = bootstrap.bind(port).sync();
            //对关闭通道进行监听
            channelFuture.channel().closeFuture();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        System.out.println("--netty server has down!");
    }

    static class MyServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            //获取客户端发送过来的消息
            ByteBuf byteBuf = (ByteBuf) msg;
            System.out.println("--in channelRead " + ctx.channel().remoteAddress() + "message：" + byteBuf.toString(CharsetUtil.UTF_8));
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            //发送消息给客户端
            String s = "服务端已收到消息，并给你发送一个问号?";
            System.out.println("--in channelReadComplete: " + s);
            ctx.writeAndFlush(Unpooled.copiedBuffer(s, CharsetUtil.UTF_8));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            //发生异常，关闭通道
            System.out.println("--in exceptionCaught");
            ctx.close();
        }

        public MyServerHandler() {
            super();
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            System.out.println("--in channelRegistered");
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            System.out.println("--in channelUnregistered");
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("--in channelUnregistered");
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("--in channelUnregistered");
            super.channelInactive(ctx);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            System.out.println("--in channelUnregistered");
            super.channelWritabilityChanged(ctx);
        }
    }

    static class MyBizHandler extends ChannelOutboundHandlerAdapter {
        @Override
        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            System.out.println("--out bind");
            super.bind(ctx, localAddress, promise);
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            System.out.println("--out connect");
            super.connect(ctx, remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            System.out.println("--out disconnect");
            super.disconnect(ctx, promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            System.out.println("--out close");
            super.close(ctx, promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            System.out.println("--out deregister");
            super.deregister(ctx, promise);
        }

        @Override
        public void read(ChannelHandlerContext ctx) throws Exception {
            System.out.println("--out read");
            super.read(ctx);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            System.out.println("--out write");
            super.write(ctx, msg, promise);
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception {
            System.out.println("--out flush");
            super.flush(ctx);
        }
    }
}
