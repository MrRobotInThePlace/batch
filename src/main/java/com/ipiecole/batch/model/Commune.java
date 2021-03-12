package com.ipiecole.batch.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
@Entity
public class Commune {
    @Id
    @Column(length = 5)
    private String codeInsee;
    private String nom;
    @Column(length = 5)
    private String codePostal;
    private Double latitude;
    private Double longitude;
    public Commune() {
    }
    public Commune(String codeInsee, String nom, String codePostal, Double latitude, Double longitude) {
        this.codeInsee = codeInsee;
        this.nom = nom;
        this.codePostal = codePostal;
        this.latitude = latitude;
        this.longitude = longitude;
    }
    public String getCodeInsee() {
        return codeInsee;
    }
    public void setCodeInsee(String codeInsee) {
        this.codeInsee = codeInsee;
    }
    public String getNom() {
        return nom;
    }
    public void setNom(String nom) {
        this.nom = nom;
    }
    public String getCodePostal() {
        return codePostal;
    }
    public void setCodePostal(String codePostal) {
        this.codePostal = codePostal;
    }
    public Double getLatitude() {
        return latitude;
    }
    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }
    public Double getLongitude() {
        return longitude;
    }
    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    @Override
    public String toString() {
        return "Commune{" +
                "codeInsee='" + codeInsee + '\'' +
                ", nom='" + nom + '\'' +
                ", codePostal='" + codePostal + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}
