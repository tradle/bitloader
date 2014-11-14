package io.tradle.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.tradle.loader.BitLoaderHandler;

public class LoaderInitializer extends ChannelInitializer<SocketChannel> {
	
	private final BitLoaderHandler bitLoader;

	public LoaderInitializer() {
		super();
		bitLoader = new BitLoaderHandler();
	}
	
    @Override
    public void initChannel(SocketChannel ch) {
       final ChannelPipeline p = ch.pipeline()
		 .addLast(new HttpRequestDecoder())
		 .addLast(new HttpObjectAggregator(1048576))
		 .addLast(new HttpResponseEncoder())
		 .addLast(new HttpContentCompressor())
		 .addLast(bitLoader);
    }
}
