package com.vcc.bigdata.aeproxy.command;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.ThreadLocalData;
import com.vcc.bigdata.aeproxy.connection.ConnectionManager;
import com.vcc.bigdata.aeproxy.databaseOP.UserDAO;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class WrapperCommand extends Command {
	public static final Logger LOG = LogManager.getLogger(WrapperCommand.class);
	public static final int INFO = 1;
	private int type;
	private Buffer buf;
	private Policy policy;
	private Cluster cluster;
	private Buffer outBuf;
	private String host;
	private Node node;

	public WrapperCommand(Cluster cluster, Policy policy, String host, Node node, Map<String, Integer> userRole, Map<String, String> userPro) {
		this.cluster = cluster;
		this.policy = policy;
		outBuf = Buffer.buffer();
		this.host = host;
		this.node = node;

	}

	public WrapperCommand(Buffer buf) {
		this.buf = buf;
	}

	public WrapperCommand setType(int type) {
		this.type = type;
		return this;
	}

	public int getType (){
		return type;
	}
	public Buffer getBuf() {
		return buf;
	}

	public WrapperCommand setBuf(Buffer buf) {
		this.buf = buf;
		return this;
	}

	@Override
	protected Node getNode() throws AerospikeException.InvalidNode {
		return null;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {

	}


	public boolean isInfoCmd() {
		return type == INFO;
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("CMD{");
		if (isInfoCmd())
			b.append("type: info");
		else {
			if (type != 3)
				LOG.error("Check: " + this);
			b.append("type: " + type);
		}
		b.append(", size: " + buf.length());
		try {
			if (isInfoCmd()) {
				String commands = '"' + buf.getBuffer(8, buf.length()).toString().trim().replace('\n', ';') + '"';
				b.append(", msg: ").append(commands);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		b.append("}");
		return b.toString();
	}

	// return 0 is auth command, 1 is create user, 2 is drop user
	private int authType(Buffer buff){
		byte[] b = buff.getBytes();
		int a = b[10] & 0xFF;
		return a;

	}

	private void write(NetSocket sock, Buffer buf , int i){
		Buffer tmp = Buffer.buffer(buf.getBytes(0, 24));
//		tmp.appendBytes(buff.getBuffer().getBytes(0, 24));
		tmp.setByte(9, (byte) i);
		sock.write(tmp);
		buf = Buffer.buffer();
	}

	private void createUser(Buffer buf, NetSocket sock, Map<String, String> user_authen, ConnectionManager adminManager){
		String[] sp = buf.toString().split("\n");
		int uIndex = sp[0].indexOf("=");
		String user = buf.getString(29, uIndex - 3);
		String pass = buf.getString(uIndex + 2, uIndex + 62);
		try {

			/*
			Step 1: check user is admin user
			Step 2: if true add

			 */
			//some op sql
			if(adminManager.isContain(sock)){
				UserDAO ud = new UserDAO();
				ud.addUser(user,pass, 3);
				user_authen.put(user, pass);
				write(sock, buf, 0);
				LOG.info("Create succeded!");
			}
			else{
				write(sock, buf, 1);
				LOG.info("Create failed!");
			}
		}
		catch (Exception e){
			e.printStackTrace();
			write(sock, buf, 1);
			LOG.info("Create failed!!");
		}
	}

	private void dropUser(Buffer buf, NetSocket sock, Map<String, String> user_authen, ConnectionManager adminManager){
		String user = buf.getString(29, buf.length());
		try {

			// some op sql
			/*
			Step 1: check user is admin user
			Step 2: if true remove

			 */
			if(adminManager.isContain(sock)){
				UserDAO ud = new UserDAO();
				ud.deleteUser(user);
				user_authen.remove(user);
				write(sock, buf, 0);
			}
			else{
				write(sock, buf, 1);
			}
		}
		catch (Exception e){
			e.printStackTrace();
			write(sock, buf, 1);
		}
	}



	private void changeRole(Buffer buf, NetSocket sock, ConnectionManager adminManager){
		String[] sp = buf.toString().split("\n");
		String user = sp[0].substring(29,  sp[0].length() - 4);
		String role = sp[1].substring(2, sp[1].length());

		LOG.info("User {} ROLE {} ", user, role);
		try {
			/*
				Step 1: check user is admin?
				Step 2: change role
			 */
			UserDAO ud = new UserDAO();
			if(adminManager.isContain(sock)){
				if(Integer.parseInt(role) == 2){
					ud.setRole4User(user, 2);
					adminManager.addClient(sock);
				}
				else{
					ud.setRole4User(user, 3);
				}
				write(sock, buf, 0);
			}
			else{
				write(sock, buf, 1);
			}


		}
		catch (Exception e){
			e.printStackTrace();
			write(sock, buf, 1);
		}
	}

	private void verifySoket(Buffer buf, NetSocket sock,ConnectionManager adminManager,Map<String, Integer> usrRole, ConnectionManager connectionManager, Map<String, String> user_authen){

		String auth = buf.toString();
		int uIndex = auth.indexOf("=");

		String user = buf.getString(29, uIndex - 3);
		String pass = buf.getString(uIndex + 2, buf.length());
		String passHash = user_authen.get(user);
		LOG.info("User {} pass {}", user, pass);

		if(user_authen.containsKey(user) && passHash.equals(pass)){
			connectionManager.addClient(sock);
			if(usrRole.get(user).equals(2)){
				adminManager.addClient(sock);
			}
			write(sock, buf, 0);
		}
		else{
			write(sock, buf, 1);
		}
	}

	private void setPassword(Buffer buf, NetSocket sock, ConnectionManager adminManager){
		int uIndex = buf.toString().indexOf("=");
		String user = buf.getString(29, uIndex - 3);
		String pass = buf.getString(uIndex + 2, uIndex + 62);

		try{

			// some sql here
			if(adminManager.isContain(sock)){
				UserDAO ud = new UserDAO();
				ud.updateUser(user, pass);
				write(sock, buf, 0);
			}
			else{
				write(sock, buf,1);
			}
		}catch (Exception e){
			e.printStackTrace();
			write(sock,buf,1);
		}


	}

	public void execCommand(ConnectionManager adminManager, Map<String, Integer> usrRole,ConnectionManager connectionManager, NetSocket sock, Map<String, String> user_authen, NetSocket clientSock){
		String command = buf.toString();
//		user_authen.put("thinhnd", "$2a$10$7EqJtq98hPqEX7fNZaFWoOYfVUs94gD2f55OKlAbxubc/KUOj3Lb6");
		if(type == 2){
			switch (authType(buf)){
				case 0:
					// check auth
					LOG.info("Verify {}",command);
					verifySoket(buf, sock, adminManager, usrRole , connectionManager, user_authen);
					break;
				case 1:
					createUser(buf, sock, user_authen,adminManager);
					// create user
					break;
				case 2:
					//drop user
					dropUser(buf, sock, user_authen,adminManager);
					break;
				case 3:
					// set pwd
					setPassword(buf, sock, adminManager);
				case 5:
					changeRole(buf, sock, adminManager);
					break;
				default:
					LOG.info("invalid command!!");
					break;
			}
			buf = Buffer.buffer();
		}
		else{
			try{
				if(true)
				{
					exec(sock,clientSock, buf);
//					sock.write(outBuf);
//					buff.setBuffer(Buffer.buffer());
//					buff.setIndex(0);
//					LOG.info("Write done!");
				}
				else{
					buf=(Buffer.buffer());
				}
			}
			catch(Exception e){
				buf=(Buffer.buffer());
				e.printStackTrace();
			}
		}
	}

	private void parseResponse() {
		String commands = buf.getBuffer(8, buf.length()).toString();
		int numCommands = commands.length()
				- commands.replace("\n", "").length();
		if (numCommands == 1){
			LOG.info("Comming");
			parseSingleResponse();}
		else
			parseMultiResponse();



		// commands to intercept
		// see more at
		// https://github.com/aerospike/aerospike-server/blob/f06a3839054f5a396a41d0aa32d6dea6e3009a17/as/src/base/thr_info.c
		// - services
		// - services-alumni
		// - peers-clear-alt //no port res
		// - peers-clear-std
		// - peers-tls-alt
		// - peers-tls-std

		// -- peers-clear-std
	}

	private void resizeBuffer(int size) {
		if (size > dataBuffer.length) {
			dataBuffer = ThreadLocalData.resizeBuffer(size);
		}
	}

	private HashMap<String, String> parseMultiResponse()
			throws AerospikeException {
		HashMap<String, String> responses = new HashMap<String, String>();
		int offset = 0;
		int begin = 0;

		// Create reusable StringBuilder for performance.
		int length = outBuf.length() - 8;
		StringBuilder sb = new StringBuilder(length);
		byte[] buffer = outBuf.getBytes(8, outBuf.length());
		while (offset < length) {
			byte b = buffer[offset];

			if (b == '\t') {
				String name = com.aerospike.client.command.Buffer.utf8ToString(
						buffer, begin, offset - begin, sb);
				checkError(name);
				begin = ++offset;

				// Parse field value.
				while (offset < length) {
					if (buffer[offset] == '\n') {
						break;
					}
					offset++;
				}

				if (offset > begin) {
					String value = com.aerospike.client.command.Buffer
							.utf8ToString(buffer, begin, offset - begin, sb);
					responses.put(name, value);
				} else {
					responses.put(name, null);
				}
				begin = ++offset;
			} else if (b == '\n') {
				if (offset > begin) {
					String name = com.aerospike.client.command.Buffer
							.utf8ToString(buffer, begin, offset - begin, sb);
					checkError(name);
					responses.put(name, null);
				}
				begin = ++offset;
			} else {
				offset++;
			}
		}

		if (offset > begin) {
			String name = com.aerospike.client.command.Buffer.utf8ToString(
					buffer, begin, offset - begin, sb);
			checkError(name);
			responses.put(name, null);
		}

		// edit and send message to client
		String res = outBuf.toString();
		if(res.contains("3000")){
			res = res.replaceAll(":3000",":5000");
			outBuf = Buffer.buffer();
			outBuf.appendBytes(res.getBytes(),0,res.getBytes().length);
		}
//
		LOG.info("Multiple  Resp: {}", res);
		return responses;
	}

	private void parseSingleResponse() throws AerospikeException {
		// Convert the UTF8 byte array into a string.
		LOG.info("Parse outBuff {}",outBuf.toString());
		byte[] buffer = outBuf.getBytes(0, outBuf.length());
		int length = outBuf.length();
		String response = com.aerospike.client.command.Buffer.utf8ToString(
				buffer, 0, length);
		response = response.replaceAll("3000","5000");
		outBuf = Buffer.buffer(response.getBytes());
		LOG.info("OutBuff Resp: {}",outBuf.toString());
//		return res;
	}

	private void checkError(String str) throws AerospikeException {
		if (str.startsWith("ERROR:")) {
			int begin = 6;
			int end = str.indexOf(':', begin);
			int code = -1;
			String message = "";

			if (end >= 0) {
				code = Integer.parseInt(str.substring(begin, end));

				if (str.charAt(str.length() - 1) == '\n') {
					message = str.substring(end + 1, str.length() - 1);
				} else {
					message = str.substring(end + 1);
				}
			}
			throw new AerospikeException(code, message);
		}
	}

	public Buffer getOutBuf() {
		return outBuf;
	}

	public void exec(NetSocket sock,NetSocket netSocket, Buffer buf) {

//		Connection conn = node.getConnection(10000);
		MsgBuffer msgBuffer = new MsgBuffer();

		LOG.info("Send buffer: {}", buf);
		netSocket.write(buf);
		netSocket.handler(dataBuffer -> {
					if(msgBuffer.getIndex() == 0){
						long size = com.aerospike.client.command.Buffer.bytesToLong(dataBuffer.getBytes(0, 8), 0);
						msgBuffer.setLength((int) (size & 0xFFFFFFFFFFFFL));
						msgBuffer.appendBuffer(dataBuffer);
						msgBuffer.incIndex();
					}
					else{
						LOG.info("Continue in server response ... ");
						msgBuffer.appendBuffer(dataBuffer);
					}
					if(msgBuffer.getBuffer().getBytes().length == msgBuffer.getLength() + 8){
						LOG.info("HERE IS ERROR!!!");
						if(msgBuffer.getBuffer().getByte(1) == (byte) 1){
							outBuf = Buffer.buffer();
							outBuf.appendBuffer(msgBuffer.getBuffer());
							parseResponse();
						}
						else{
							outBuf = Buffer.buffer();
							outBuf.appendBuffer(msgBuffer.getBuffer());
						}
						LOG.info("MESSAGE {}", msgBuffer.getBuffer().toString());
						LOG.info("BYTE MSG {}", BytesParser.parse(msgBuffer.getBuffer()));
						LOG.info("Out buf: {}", outBuf.toString());
						try {
							sock.write(outBuf);
						}
						catch (Exception e){
							e.printStackTrace();
						}
						outBuf = Buffer.buffer();
						msgBuffer.clear();
					}
				});
			}
//		try {
//			sendCommand(conn);
//
//			if (isInfoCmd()) {
//				// get first node
//				readInfoResponse(conn);
//			} else {
//				handleMsgCommand(conn);
//			}
//			node.putConnection(conn);
//		} catch (Exception re) {
//			node.closeConnection(conn);
//
//		}

//	}

	private void readInfoResponse(Connection conn) {
		try {
			// read header
			dataBuffer = ThreadLocalData.getBuffer(); // create buffer to store
														// data
			resizeBuffer(8);

			// read info command
			conn.readFully(dataBuffer, 8);
			outBuf.appendBytes(dataBuffer, 0, 8);
			// read size
			long size = com.aerospike.client.command.Buffer.bytesToLong(
					dataBuffer, 0);
			int length = (int) (size & 0xFFFFFFFFFFFFL); // last 6 bytes
			LOG.info("Message length: {}", length);
			resizeBuffer(length);
			conn.readFully(dataBuffer, length);
			outBuf.appendBytes(dataBuffer, 0, length);
			LOG.info("Recive {}",outBuf.toString());

			// for debug
			parseResponse();
		} catch (Exception e) {
			LOG.debug("Err call :{}", toString());
			//e.printStackTrace();
		}
	}

	private void handleMsgCommand(Connection conn) {
		try {
			// read header
			dataBuffer = ThreadLocalData.getBuffer(); // create buffer to store
														// data
			resizeBuffer(8);
			// read info command
			try {
				conn.readFully(dataBuffer, 8);
				outBuf.appendBytes(dataBuffer, 0, 8);
				long size = com.aerospike.client.command.Buffer.bytesToLong(dataBuffer, 0);
				int length = (int) (size & 0xFFFFFFFFFFFFL); // last 6 bytes
				resizeBuffer(length);
				conn.readFully(dataBuffer, length);
				outBuf.appendBytes(dataBuffer, 0, length);
				String res = outBuf.toString();
				if (res.contains(":3000")) {
					res = res.replaceAll(":3000", ":5000");
					LOG.info("Contain port: {}", res);
					outBuf = Buffer.buffer(res.getBytes());
//					outBuf.appendBytes(res.getBytes(), 0, res.getBytes().length);
					LOG.info("Send Done!!");
				}
			}
			catch(Exception e){
				LOG.info("Send fail!!");
				e.printStackTrace();
			}

			// parseResponse();

		} catch (Exception e) {
			LOG.info("Buff: {}", outBuf.toString());
			LOG.info("DATA: {}", com.aerospike.client.command.Buffer
					.utf8ToString(dataBuffer, 0, dataBuffer.length));
			LOG.debug("Err call :{}", toString());
//			e.printStackTrace();
		}
	}

	private void sendCommand(Connection conn) throws AerospikeException {
		try {
			conn.write(buf.getBytes(), buf.length());
			LOG.info("send: {} bytes", buf.length());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
