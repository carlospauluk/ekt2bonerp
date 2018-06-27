<?php 

class Ektencomenda_model extends CI_Model {
    
    
    public function truncate_table() {
        $sql = "TRUNCATE TABLE ekt_encomenda";
        return $this->db->query($sql);
    }
    
    
}