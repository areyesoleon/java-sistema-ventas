/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gt.com.lighthouse.sistema.pojos;

import java.io.File;

/**
 *
 * @author oscarreyes
 */
public class Producto {
    private String idProducto;
    private String nombreProducto;
    private String descProducto;
    private double stockProducto;
    private File fotoProducto;
    private String unidadProducto;
    private double precioCompraProducto;
    private double precioVentaProducto;
    private double existenciasProducto;
    private int idCategoria;
    private int idProveedor;

    public Producto(String idProducto, String nombreProducto, String descProducto, double stockProducto, File fotoProducto, String unidadProducto, double precioCompraProducto, double precioVentaProducto, double existenciasProducto, int idCategoria, int idProveedor) {
        this.idProducto = idProducto;
        this.nombreProducto = nombreProducto;
        this.descProducto = descProducto;
        this.stockProducto = stockProducto;
        this.fotoProducto = fotoProducto;
        this.unidadProducto = unidadProducto;
        this.precioCompraProducto = precioCompraProducto;
        this.precioVentaProducto = precioVentaProducto;
        this.existenciasProducto = existenciasProducto;
        this.idCategoria = idCategoria;
        this.idProveedor = idProveedor;
    }

    public String getIdProducto() {
        return idProducto;
    }

    public void setIdProducto(String idProducto) {
        this.idProducto = idProducto;
    }

    public String getNombreProducto() {
        return nombreProducto;
    }

    public void setNombreProducto(String nombreProducto) {
        this.nombreProducto = nombreProducto;
    }

    public String getDescProducto() {
        return descProducto;
    }

    public void setDescProducto(String descProducto) {
        this.descProducto = descProducto;
    }

    public double getStockProducto() {
        return stockProducto;
    }

    public void setStockProducto(double stockProducto) {
        this.stockProducto = stockProducto;
    }

    public File getFotoProducto() {
        return fotoProducto;
    }

    public void setFotoProducto(File fotoProducto) {
        this.fotoProducto = fotoProducto;
    }

    public String getUnidadProducto() {
        return unidadProducto;
    }

    public void setUnidadProducto(String unidadProducto) {
        this.unidadProducto = unidadProducto;
    }

    public double getPrecioCompraProducto() {
        return precioCompraProducto;
    }

    public void setPrecioCompraProducto(double precioCompraProducto) {
        this.precioCompraProducto = precioCompraProducto;
    }

    public double getPrecioVentaProducto() {
        return precioVentaProducto;
    }

    public void setPrecioVentaProducto(double precioVentaProducto) {
        this.precioVentaProducto = precioVentaProducto;
    }

    public double getExistenciasProducto() {
        return existenciasProducto;
    }

    public void setExistenciasProducto(double existenciasProducto) {
        this.existenciasProducto = existenciasProducto;
    }

    public int getIdCategoria() {
        return idCategoria;
    }

    public void setIdCategoria(int idCategoria) {
        this.idCategoria = idCategoria;
    }

    public int getIdProveedor() {
        return idProveedor;
    }

    public void setIdProveedor(int idProveedor) {
        this.idProveedor = idProveedor;
    }
    @Override
    public String toString(){
        return this.idProducto;
    }

        
}
