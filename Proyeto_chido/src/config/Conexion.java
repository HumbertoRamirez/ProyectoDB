package config;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Método de conexión hacia la base de datos.
 *
 * @author Gabriel Barrón Rodríguez.
 */
public class Conexion {

    private static Conexion con = new Conexion();
    private Connection conn = null;
    private Statement stmt;
    private ResultSet rst;
    private DatabaseMetaData metaDatos;
    private ResultSetMetaData rstMetadatos;

    private Conexion() {
        Properties prop = new Properties();
        InputStream input = null;
        try {

            input = getClass().getClassLoader().getResourceAsStream("database.properties");

            if (input != null) {
                prop.load(input);
            }
            String usuario = prop.getProperty("user");
            String pwd = prop.getProperty("pwd");
            String database = prop.getProperty("database");
            String url = prop.getProperty("url") + database;
            System.out.println("Url:" + url);
            Class.forName("com.mysql.jdbc.Driver");

            this.conn = DriverManager.getConnection(url, usuario, pwd);

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public int consultaUsuario(String query) {
        int total = 0;
        try {

            
            stmt = conn.createStatement();
            rst = stmt.executeQuery(query);
            
            rst.first();
            rst.last();
            
            total = rst.getRow();
            
        } catch (SQLException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        } 
        return total;
    }

    public Object[] query(String tabla) {
        String qry = "Select * from " + tabla;
        int col = 0;
        Object[][] data = null;
        String[] header = null;
        Object[] result = new Object[2];
        
        try {
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rst = stmt.executeQuery(qry);
            rstMetadatos = rst.getMetaData();
            rst.last();
            col = rstMetadatos.getColumnCount();
            System.out.println("Total " + rst.getRow());
            data = new Object[rst.getRow()][col];
            header = new String[col];
            
            for (int i=1; i <= col; i++) {
                header[i-1] = rstMetadatos.getColumnName(i);
            }
            result[1] = header;
            
            int row=0;
            rst.beforeFirst();
            while(rst.next()) {
                System.out.println("entro");
                for (int c=0; c < col; c++)
                    data[row][c] = rst.getString(header[c]);

                row++;
            }
            result[0] = data;
        } catch (SQLException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return result;
    }
    public static Conexion getInstance() {
        return con;
    }
}
