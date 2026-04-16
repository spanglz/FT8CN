package com.bg7yoz.ft8cn.connector;

import android.util.Log;

import com.bg7yoz.ft8cn.database.ControlMode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * WiFi CAT connector for ESP32-based TCP bridge (e.g. TX-500 via ESP32 AP at 192.168.4.1:8899).
 * Speaks Kenwood CAT protocol over a plain TCP socket.
 */
public class TcpCatConnector extends BaseRigConnector {
    private static final String TAG = "TcpCatConnector";
    private static final int CONNECT_TIMEOUT_MS = 5000;

    private final String host;
    private final int port;

    private Socket socket;
    private OutputStream outputStream;
    private Thread readThread;
    private volatile boolean running = false;
    private final ExecutorService sendExecutor = Executors.newSingleThreadExecutor();

    public TcpCatConnector(String host, int port, int controlMode) {
        super(controlMode);
        this.host = host;
        this.port = port;
    }

    @Override
    public void connect() {
        running = true;
        readThread = new Thread(() -> {
            try {
                socket = new Socket();
                socket.connect(new java.net.InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
                socket.setSoTimeout(0);
                outputStream = socket.getOutputStream();
                getOnConnectorStateChanged().onConnected();

                InputStream in = socket.getInputStream();
                byte[] buf = new byte[1024];
                int len;
                while (running && !socket.isClosed()) {
                    len = in.read(buf);
                    if (len < 0) break;
                    if (len > 0 && getOnConnectReceiveData() != null) {
                        byte[] data = new byte[len];
                        System.arraycopy(buf, 0, data, 0, len);
                        Log.d(TAG, "RX: " + new String(data));
                        getOnConnectReceiveData().onData(data);
                    }
                }
            } catch (IOException e) {
                if (running) {
                    Log.e(TAG, "TCP error: " + e.getMessage());
                    getOnConnectorStateChanged().onRunError("TCP CAT error: " + e.getMessage());
                }
            } finally {
                closeQuietly();
                if (running) {
                    getOnConnectorStateChanged().onDisconnected();
                }
            }
        }, "TcpCatReader");
        readThread.setDaemon(true);
        readThread.start();
    }

    @Override
    public void disconnect() {
        running = false;
        sendExecutor.shutdown();
        closeQuietly();
        getOnConnectorStateChanged().onDisconnected();
    }

    @Override
    public void sendData(byte[] data) {
        final byte[] copy = data.clone();
        sendExecutor.execute(() -> {
            if (outputStream == null) return;
            try {
                Log.d(TAG, "TX: " + new String(copy));
                outputStream.write(copy);
                outputStream.flush();
            } catch (IOException e) {
                Log.e(TAG, "sendData error: " + e.getMessage());
                getOnConnectorStateChanged().onRunError("TCP send error: " + e.getMessage());
            }
        });
    }

    @Override
    public void setPttOn(byte[] command) {
        sendData(command);
    }

    private void closeQuietly() {
        try {
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException ignored) {
        }
        outputStream = null;
    }
}
