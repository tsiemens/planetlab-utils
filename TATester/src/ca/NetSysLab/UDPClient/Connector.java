package ca.NetSysLab.UDPClient;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import javax.xml.bind.DatatypeConverter;

public class Connector {
  DatagramSocket clientSocket = null;
  static boolean DEBUG = false;
  String testAddy;
  int testPort;
  RecvHelper rh;
  
  /**
   * 
   * @param a
   * @param p
   */
  public Connector(String a, int p) {
    try {
      clientSocket = new DatagramSocket();
      //clientSocket.setSoTimeout(5000);
    } catch (SocketException e) { e.printStackTrace(); }
    this.testAddy = a;
    this.testPort = p;
    rh = new RecvHelper(clientSocket);
    new Thread(rh).start();
  }
  
  /**
   * 
   * @param a
   * @param p
   * @param clientPort
   */
  public Connector(String a, int p, int clientPortNumber) {
    try {
      clientSocket = new DatagramSocket(clientPortNumber);
      //clientSocket.setSoTimeout(5000);
    } catch (SocketException e) { e.printStackTrace(); }
    this.testAddy = a;
    this.testPort = p;
    rh = new RecvHelper(clientSocket);
    new Thread(rh).start();
  }
  
  /**
   * Does not launch a receiver thread.
   * @param a
   * @param p
   * @param waitForResponse - dummy.
   */
  public Connector(String a, int p, boolean waitForResponse) {
    try {
      clientSocket = new DatagramSocket();
      //clientSocket.setSoTimeout(5000);
    } catch (SocketException e) { e.printStackTrace(); }
    this.testAddy = a;
    this.testPort = p;
    //rh = new RecvHelper(clientSocket);
    //new Thread(rh).start();
  }
  
  /**
   * Close the socket.
   */
  public void close() {
	  clientSocket.close();	  
  }
  
  /**
   * Send and receive.
   * @param pack
   * @return
   */
  public byte[] sendAndRecieve(byte[] pack) {
    DatagramPacket dpack = null;
    try {
      dpack = new DatagramPacket(pack, pack.length, InetAddress.getByName(testAddy), testPort);
    } catch (UnknownHostException e) { e.printStackTrace(); }

    if (DEBUG) System.out.println("SEND: " + DatatypeConverter.printHexBinary(pack));
    try {
      clientSocket.send(dpack);
    } catch (IOException e) { e.printStackTrace(); }

    ByteBuffer bb = ByteBuffer.wrap(pack);
    long temp = bb.getLong();
    byte[] receiveData = rh.request(temp);

    //if (DEBUG) System.out.println("RECV: " + DatatypeConverter.printHexBinary(receiveData));
    return receiveData;
  }
  
  /**
   * Send only.
   * @param pack
   */
  public void send(byte[] pack) {
	    DatagramPacket dpack = null;
	    try {
	      dpack = new DatagramPacket(pack, pack.length, InetAddress.getByName(testAddy), testPort);
	    } catch (UnknownHostException e) { e.printStackTrace(); }

	    if (DEBUG) System.out.println("SEND: " + DatatypeConverter.printHexBinary(pack));
	    try {
	      clientSocket.send(dpack);
	    } catch (IOException e) { e.printStackTrace(); }

	    //ByteBuffer bb = ByteBuffer.wrap(pack);
	    //long temp = bb.getLong();
	    //byte[] receiveData = rh.request(temp);

	    //if (DEBUG) System.out.println("RECV: " + DatatypeConverter.printHexBinary(receiveData));
	    //return receiveData;
	  }
}
