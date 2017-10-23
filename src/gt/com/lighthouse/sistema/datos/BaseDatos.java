/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gt.com.lighthouse.sistema.datos;

import gt.com.lighthouse.sistema.pojos.CategoriaProd;
import gt.com.lighthouse.sistema.pojos.DetalleVenta;
import gt.com.lighthouse.sistema.pojos.Producto;
import gt.com.lighthouse.sistema.pojos.Proveedores;
import gt.com.lighthouse.sistema.pojos.Ventas;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author oscarreyes
 */
public class BaseDatos {
        Connection conn = null;
        PreparedStatement prepSt = null;
        Statement st = null;
        ResultSet rs = null;
    public BaseDatos(){
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
    }
    
    public void insertarProducto(Producto producto) throws FileNotFoundException{
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            File fileFoto = producto.getFotoProducto();
            FileInputStream fis = new FileInputStream(fileFoto);
            try {
                fis = new FileInputStream(producto.getFotoProducto());
            } catch (FileNotFoundException ex) {
                Logger.getLogger(BaseDatos.class.getName()).log(Level.SEVERE, null, ex);
            }
            String sql = "INSERT INTO cat_productos (" +
                    "	id_prod, nombre_prod, desc_prod, stock_prod, foto_prod, unidad_prod, precio_compra_prod, precio_venta_prod, existencias_prod, id_categoria_prod, id_proveedor) " +
                    "	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            prepSt = conn.prepareStatement(sql);
            prepSt.setString(1,producto.getIdProducto());
            prepSt.setString(2, producto.getNombreProducto());
            prepSt.setString(3, producto.getDescProducto());
            prepSt.setDouble(4, producto.getStockProducto());
            long tamanoFoto = fileFoto.length();
            prepSt.setBinaryStream(5,fis,tamanoFoto);
            prepSt.setString(6, producto.getUnidadProducto());
            prepSt.setDouble(7, producto.getPrecioCompraProducto());
            prepSt.setDouble(8, producto.getPrecioVentaProducto());
            prepSt.setDouble(9, producto.getExistenciasProducto());
            prepSt.setInt(10, producto.getIdCategoria());
            prepSt.setInt(11, producto.getIdProveedor());
            
            prepSt.executeUpdate();
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    public void actualizarInventario(Producto producto, double cantidad){
        try {
            String sql = "UPDATE cat_productos SET existencias_prod = ? WHERE id_prod=?";
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            prepSt = conn.prepareStatement(sql);
            
            prepSt.setDouble(1, cantidad);
            prepSt.setString(2, producto.getIdProducto());
            
            prepSt.execute();
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }   
    }
    
    
    public void insertarCategoriaProducto(CategoriaProd categoria){
        String sql = "INSERT INTO cat_categorias (nom_categoria_prod, desc_categoria_prod) VALUES (?, ?)";
        try {
            
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            prepSt = conn.prepareStatement(sql);
            
            prepSt.setString(1, categoria.getNomCategoriaProd());
            prepSt.setString(2, categoria.getDescCategoriaProd());
            
            prepSt.execute();
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    public void insertarProveedor(Proveedores proveedor){
        
        String sql = "INSERT INTO cat_proveedores (" +
                    "	nom_proveedor, dir_proveedor, telefono_proveedor, email_proveedor, contacto_proveedor) " +
                    "	VALUES (?, ?, ?, ?, ?)";
        
        try {
            
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            prepSt = conn.prepareStatement(sql);
            
            prepSt.setString(1, proveedor.getNombreProveedor());
            prepSt.setString(2,proveedor.getDirProveedor());
            prepSt.setString(3,proveedor.getTelefonoProveedor());
            prepSt.setString(4,proveedor.getEmailProveedor());
            prepSt.setString(5, proveedor.getContactoProveedor());
            
            prepSt.execute();
            
        } catch (SQLException ex) {
            Logger.getLogger(BaseDatos.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                Logger.getLogger(BaseDatos.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
    }
    
    public void insertarVenta(Ventas venta){
        
        String sql = "﻿INSERT INTO ventas (" +
                    "	monto_venta, fecha_venta) " +
                    "	VALUES (?, ?)";
        
        try {
            
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            prepSt = conn.prepareStatement(sql);
            
            prepSt.setDouble(1, venta.getMontoVenta());
            prepSt.setDate(2, venta.getFechaVenta());
            prepSt.execute();
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                Logger.getLogger(BaseDatos.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
    }
    
    public void insertarDetalleVenta(DetalleVenta detalle){
        
        String sql = "﻿INSERT INTO detalle_venta(" +
                    "	 id_venta, id_prod, cantidad_vendida)" +
                    "	VALUES (?, ?, ?);";
        
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            prepSt = conn.prepareStatement(sql);
            
            prepSt.setInt(1, detalle.getIdVenta());
            prepSt.setInt(2, detalle.getIdProd());
            prepSt.setDouble(3, detalle.getCantidadVendida());
            prepSt.execute();
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                Logger.getLogger(BaseDatos.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
    }
    
    public ArrayList<Producto> obtenerProductos(){
        ArrayList <Producto> listaProductos = new ArrayList <Producto>();
        try {
            String sql = "SELECT * FROM cat_productos order by nombre_prod";
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            prepSt = conn.prepareStatement(sql);
            rs = prepSt.executeQuery();
            
            while(rs.next()){
                String id = rs.getString("id_prod");
                String nombre = rs.getString("nombre_prod");
                String descripcion = rs.getString("desc_prod");
                double stock = rs.getDouble("stock_prod");
                String unidad = rs.getString("unidad_prod");
                double precioCompra = rs.getDouble("precio_compra_prod");
                double precioVenta = rs.getDouble("precio_venta_prod");
                double existencias = rs.getDouble("existencias_prod");
                int idCategoria = rs.getInt("id_categoria_prod");
                int idProveedor = rs.getInt("id_proveedor");
                
                Producto producto = new Producto(id,nombre,descripcion,stock,null,unidad,precioCompra,precioVenta,existencias,idCategoria,idProveedor);
                listaProductos.add(producto);
                
            }
            
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return listaProductos;
    }
    
    public void actualizarProducto(Producto producto, boolean cambiarFoto){
            try {
                conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
                
                if(cambiarFoto == true){
                    File fileFoto = producto.getFotoProducto();
                    FileInputStream fis = new FileInputStream(fileFoto);
                    String sql = "UPDATE cat_productos SET desc_prod=?, stock_prod=?, foto_prod=?, unidad_prod=?,"+
                                 "precio_compra_prod=?, precio_venta_prod=?,id_categoria_prod=?,id_proveedor=?"+
                                 "WHERE id_prod=?";
                    prepSt = conn.prepareStatement(sql);
                    
                    prepSt.setString(1, producto.getDescProducto());
                    prepSt.setDouble(2, producto.getStockProducto());
                    long tamanoFoto = fileFoto.length();
                    prepSt.setBinaryStream(3,fis,tamanoFoto);
                    prepSt.setString(4, producto.getUnidadProducto());
                    prepSt.setDouble(5, producto.getPrecioCompraProducto());
                    prepSt.setDouble(6, producto.getPrecioVentaProducto());
                    prepSt.setInt(7, producto.getIdCategoria());
                    prepSt.setInt(8, producto.getIdProveedor());
                    prepSt.setString(9, producto.getIdProducto());
                }
                else{
                    String sql = "UPDATE cat_productos SET desc_prod=?, stock_prod=?,  unidad_prod=?,"+
                                 "precio_compra_prod=?, precio_venta_prod=?,id_categoria_prod=?,id_proveedor=?"+
                                 "WHERE id_prod=?";
                    prepSt = conn.prepareStatement(sql);
                    
                    prepSt.setString(1, producto.getDescProducto());
                    prepSt.setDouble(2, producto.getStockProducto());
                    prepSt.setString(3, producto.getUnidadProducto());
                    prepSt.setDouble(4, producto.getPrecioCompraProducto());
                    prepSt.setDouble(5, producto.getPrecioVentaProducto());
                    prepSt.setInt(6, producto.getIdCategoria());
                    prepSt.setInt(7, producto.getIdProveedor());
                    prepSt.setString(8, producto.getIdProducto());
                }
                prepSt.executeUpdate();
            } catch (SQLException ex) {
                ex.printStackTrace();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(BaseDatos.class.getName()).log(Level.SEVERE, null, ex);
            }
    }
    
    public ArrayList<Producto> obtenerProductosPorCriterio(String criterio){
        ArrayList <Producto> listaProductos = new ArrayList <Producto>();
        try {
            String sql = "SELECT * FROM cat_productos WHERE id_prod LIKE '%"+criterio+"%' OR nombre_prod LIKE '%"+criterio+"%' ORDER BY nombre_prod";
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            st = conn.createStatement();
            rs = st.executeQuery(sql);
            
            while(rs.next()){
                String id = rs.getString("id_prod");
                String nombre = rs.getString("nombre_prod");
                String descripcion = rs.getString("desc_prod");
                double stock = rs.getDouble("stock_prod");
                String unidad = rs.getString("unidad_prod");
                double precioCompra = rs.getDouble("precio_compra_prod");
                double precioVenta = rs.getDouble("precio_venta_prod");
                double existencias = rs.getDouble("existencias_prod");
                int idCategoria = rs.getInt("id_categoria_prod");
                int idProveedor = rs.getInt("id_proveedor");
                
                Producto producto = new Producto(id,nombre,descripcion,stock,null,unidad,precioCompra,precioVenta,existencias,idCategoria,idProveedor);
                listaProductos.add(producto);
                
            }
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } 
            return listaProductos;
    }
    
    public void borrarProducto(Producto producto){
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            String sql = "DELETE FROM cat_productos WHERE id_prod = ?";
            prepSt = conn.prepareStatement(sql);
            
            prepSt.setString(1, producto.getIdProducto());
            prepSt.executeUpdate();
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    public ArrayList<CategoriaProd> obtenerCategorias(){
        
        ArrayList<CategoriaProd> listaCategorias = new ArrayList<CategoriaProd>();
        
        
        try {
            String sql = "SELECT * FROM cat_categorias";
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            prepSt = conn.prepareStatement(sql);
            prepSt.executeQuery();
            rs = prepSt.executeQuery();
            
            while(rs.next()){
                int id = rs.getInt("id_categoria_prod");
                String nombre = rs.getString("nom_categoria_prod");
                String descripcion = rs.getString("desc_categoria_prod");
                
                CategoriaProd categoria = new CategoriaProd(id, nombre, descripcion);
                listaCategorias.add(categoria);
            }
            
            
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return listaCategorias;
    }
    
    public ArrayList<Proveedores> obtenerProveedores(){
        ArrayList<Proveedores> listaProveedores = new ArrayList<Proveedores>();
        try {
            String sql = "SELECT * FROM cat_proveedores";
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
            prepSt = conn.prepareStatement(sql);
            rs = prepSt.executeQuery();
            
            while(rs.next()){
                int id = rs.getInt("id_proveedor");
                String nombre = rs.getString("nom_proveedor");
                String direccion = rs.getString("dir_proveedor");
                String telefono = rs.getString("telefono_proveedor");
                String email = rs.getString("email_proveedor");
                String contacto = rs.getString("contacto_proveedor");
                Proveedores proveedor = new Proveedores(id,nombre,direccion,telefono,email,contacto);
                listaProveedores.add(proveedor);
            }
            
        } catch (SQLException ex) {
            Logger.getLogger(BaseDatos.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                prepSt.close();
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return listaProveedores;
    }
    
    public InputStream buscarFoto(Producto producto){
            InputStream streamFoto = null;
            try {
                conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db-sistemas","postgres","Ithinkdifferent!");
                String sql = "SELECT foto_prod FROM cat_productos WHERE id_prod = ?";
                
                prepSt = conn.prepareStatement(sql);
                prepSt.setString(1, producto.getIdProducto());
                rs = prepSt.executeQuery();
                
                while(rs.next()){
                    streamFoto = rs.getBinaryStream("foto_prod");
                }
                
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            return streamFoto;
        
    }
//    public static void main(String[] args){
//        CategoriaProd categoria = new CategoriaProd(1, "Categoria prueba dos", "Descripcion de la categoria de prueba dos");
//        BaseDatos base = new BaseDatos();
//        base.insertarCategoriaProducto(categoria);
//    }
}
