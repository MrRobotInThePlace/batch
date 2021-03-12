package com.ipiecole.batch.repository;

import com.ipiecole.batch.model.Commune;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
public interface CommuneRepository extends JpaRepository<Commune, String> {
    @Query("select count(distinct c.codePostal) from Commune c")
    long countDistinctCodePostal();
}
