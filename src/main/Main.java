
package main;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.OutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {
    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }

    // Defaults (override with env vars in CBH deployment)
    static final String DB_URL  = env("DB_URL",  "jdbc:postgresql://database-lab1:5432/flem_clinic");
    static final String DB_USER = env("DB_USER", "admin");
    static final String DB_PASS = env("DB_PASS", "password");

    // Never throws — handles IO internally, so no “unhandled” errors
    static void send(HttpExchange ex, int code, String body) {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        try {
            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(code, b.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(b); }
        } catch (IOException ioe) {
            ioe.printStackTrace(); // optional: log somewhere
        } finally {
            ex.close();
        }
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(env("PORT", "8080")); // CBH injects PORT
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        // GET /db-test -> "Connection established" or "Connection failed: ..."
        server.createContext("/db-test", ex -> {
            try (Connection c = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
                send(ex, 200, "Connection established:):):)");
            } catch (SQLException e) {
                send(ex, 500, "Connection failed: " + e.getMessage());
            }
        });

        server.start();
        System.out.println("Listening on " + port);
    }
}
