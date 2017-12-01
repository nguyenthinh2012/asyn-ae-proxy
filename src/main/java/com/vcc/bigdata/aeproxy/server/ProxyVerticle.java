package com.vcc.bigdata.aeproxy.server;

import com.aerospike.client.Host;
import com.vcc.bigdata.aeproxy.command.*;
import com.vcc.bigdata.aeproxy.connection.ConnectionManager;
import com.vcc.bigdata.aeproxy.databaseOP.UserDAO;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

import java.lang.reflect.Array;
import java.util.*;

public class ProxyVerticle extends AbstractVerticle {
	public static final Logger LOG = LogManager.getLogger(ProxyVerticle.class);
	private String host = "127.0.0.1" ;
	private int port = 5000;
	private Map<String, String> user_authen;
	private Map<String, Integer> userRole;
	private boolean reusePort = true;
	private boolean tcpFastOpen = false;
	private boolean tcpCork = false;
	private boolean tcpQuickAck = false;
	Queue<NetSocket> sockQueue = new LinkedList<>();
	Queue<WrapperCommand> command = new LinkedList<>();
	public ProxyVerticle(){
		user_authen = new HashMap<>();
		userRole = new HashMap<>();
		user_authen.put("admin", "$2a$10$7EqJtq98hPqEX7fNZaFWoOrRRj2wNVy1XP7S7eSgCCEX5IGcu9hu2");
		userRole.put("admin", 2);
	}
	public ProxyVerticle(String host, int port, boolean reusePort, boolean tcpFastOpen, boolean tcpCork, boolean tcpQuickAck) {
		this.host = host;
		this.port = port;
		this.reusePort = reusePort;
		this.tcpFastOpen = tcpFastOpen;
		this.tcpCork = tcpCork;
		this.tcpQuickAck = tcpQuickAck;
		// load data from sql here

		UserDAO ud = new UserDAO();
		user_authen = new HashMap<>();
		userRole = new HashMap<>();
		user_authen.put("admin", "$2a$10$7EqJtq98hPqEX7fNZaFWoOrRRj2wNVy1XP7S7eSgCCEX5IGcu9hu2");
		userRole.put("admin", 2);
//		user_authen = ud.getAllUsersAndPasswords();
//		userRole = ud.getAllUsersAndRoles();
		LOG.info("total user {}", user_authen.size());

	}
	ConnectionManager connectionManager;
	ConnectionManager adminManager;


	@Override
	public void start() throws Exception {

		NetServerOptions options =new NetServerOptions().setTcpFastOpen(tcpFastOpen).setReuseAddress(true)
				.setReusePort(reusePort).setTcpCork(tcpCork).setTcpQuickAck(tcpQuickAck);

		NetServer server = vertx.createNetServer(options);

		// client to connect to server;
		ClientWrapper client = new ClientWrapper(null, new Host(host, 3000));
		CommandParser parser = new CommandParser(client, host);
		connectionManager = new ConnectionManager();
		adminManager = new ConnectionManager();
		NetClient netClient;

		netClient = vertx.createNetClient();
		Future<NetSocket> f = Future.future();
		netClient.connect(3000,"127.0.0.1", f);

		f.setHandler(x1 -> {
			if (x1.failed()){
				LOG.error("Could not create connection to server");
				return;
			} else {
				NetSocket asSock = x1.result();

				MsgBuffer msgBuffer1 = new MsgBuffer();
				asSock.handler(x -> {
					if(msgBuffer1.getIndex() == 0){
						long size = com.aerospike.client.command.Buffer.bytesToLong(
								x.getBytes(0, 8), 0);
						msgBuffer1.setLength((int) (size & 0xFFFFFFFFFFFFL));

						msgBuffer1.appendBuffer(x);
						msgBuffer1.incIndex();
					}
					else{
						msgBuffer1.appendBuffer(x);
						System.out.println(msgBuffer1.getBuffer().getBytes().length + " " + msgBuffer1.getLength());
					}
					if(msgBuffer1.getLength() + 8 == msgBuffer1.getBuffer().length()){

						LOG.info("RECEIVE response");
						if(msgBuffer1.getBuffer().toString().contains("3000")){
							LOG.info("INFO MSG {} ", msgBuffer1.getBuffer().toString());
							String res = msgBuffer1.getBuffer().toString().replaceAll("3000","5000");
							NetSocket netSocket = sockQueue.poll();
							netSocket.write(Buffer.buffer().appendBytes(res.getBytes()));
							msgBuffer1.clear();
						}
						else{
							NetSocket netSocket = sockQueue.poll();
							LOG.info("QUEUE SIZE {} {} ",sockQueue.size() , command.size());
							netSocket.write(msgBuffer1.getBuffer());
							msgBuffer1.clear();
						}
					}

				});
				server.connectHandler(sock -> {
					// handler new coming data
					MsgBuffer msgBuffer = new MsgBuffer();

					sock.handler(buf -> {
						//get size of message received
						if(msgBuffer.getIndex() == 0){
							long size = com.aerospike.client.command.Buffer.bytesToLong(
									buf.getBytes(0, 8), 0);
							msgBuffer.setLength((int) (size & 0xFFFFFFFFFFFFL));

							msgBuffer.appendBuffer(buf);
							msgBuffer.incIndex();
						}
						else{

							LOG.info("Continue ... ");
							msgBuffer.appendBuffer(buf);
							System.out.println(msgBuffer.getBuffer().getBytes().length + " " + msgBuffer.getLength());
						}

						if(msgBuffer.getBuffer().getBytes().length == msgBuffer.getLength() + 8){
							LOG.info("MSG comming {}", msgBuffer.getBuffer().toString());
//					vertx.executeBlocking(f ->{
							WrapperCommand cmd = parser.parse(msgBuffer.getBuffer(), userRole, user_authen);

							LOG.info("CMD {}", msgBuffer.getBuffer().toString());
							if(cmd.getType() == 2){
								LOG.info("Edsnbnm");
								Buffer tmp = msgBuffer.getBuffer().getBuffer(0,24);
								tmp.setByte(9,(byte)0);
								sock.write(tmp);
								msgBuffer.clear();
							}
							else{
								LOG.info("XVxcbxcbvbxcvbxcvb");
								command.add(cmd);
								sockQueue.add(sock);
								if(command.size() > 0){
									WrapperCommand comm = command.poll();
									asSock.write(comm.getBuf());
								}
								LOG.info("Send to server!");
								msgBuffer.clear();
							}

//						cmd.execCommand(netClient,msgBuffer,adminManager, userRole, connectionManager, sock, user_authen);

//					},false,t ->{
//					});
						}
						// LOG.info("resp size: {}", cmd.getOutBuf().length());
					});

					sock.closeHandler(x -> {
						LOG.info("End handle");
						connectionManager.remove(sock);
						adminManager.remove(sock);
						msgBuffer.clear();
					});

				});
			}
			server.listen(port, x -> {
				if (x.succeeded()) {
					LOG.info("Server start to listen at port {}", port);
				} else {
					LOG.error("Error: ", x.cause());
				}
			});
		});
		// handle client connection


		// start server

	}
}
