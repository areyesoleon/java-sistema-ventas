/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gt.com.lighthouse.sistema.pojos;

/**
 *
 * @author oscarreyes
 */
public class Proveedores {
    
    private int idProveedor;
    private String nombreProveedor;
    private String dirProveedor;
    private String telefonoProveedor;
    private String emailProveedor;
    private String contactoProveedor;

    public Proveedores(int idProveedor, String nombreProveedor, String dirProveedor, String telefonoProveedor, String emailProveedor, String contactoProveedor) {
        this.idProveedor = idProveedor;
        this.nombreProveedor = nombreProveedor;
        this.dirProveedor = dirProveedor;
        this.telefonoProveedor = telefonoProveedor;
        this.emailProveedor = emailProveedor;
        this.contactoProveedor = contactoProveedor;
    }

    public String getContactoProveedor() {
        return contactoProveedor;
    }

    public void setContactoProveedor(String contactoProveedor) {
        this.contactoProveedor = contactoProveedor;
    }

    public int getIdProveedor() {
        return idProveedor;
    }

    public void setIdProveedor(int idProveedor) {
        this.idProveedor = idProveedor;
    }

    public String getNombreProveedor() {
        return nombreProveedor;
    }

    public void setNombreProveedor(String nombreProveedor) {
        this.nombreProveedor = nombreProveedor;
    }

    public String getDirProveedor() {
        return dirProveedor;
    }

    public void setDirProveedor(String dirProveedor) {
        this.dirProveedor = dirProveedor;
    }

    public String getTelefonoProveedor() {
        return telefonoProveedor;
    }

    public void setTelefonoProveedor(String telefonoProveedor) {
        this.telefonoProveedor = telefonoProveedor;
    }

    public String getEmailProveedor() {
        return emailProveedor;
    }

    public void setEmailProveedor(String emailProveedor) {
        this.emailProveedor = emailProveedor;
    }
    
    @Override
    public String toString(){
        return this.nombreProveedor;
    }
    
}
