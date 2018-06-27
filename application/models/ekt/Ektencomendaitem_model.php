<?php 

class Ektencomendaitem_model extends CI_Model {
    
    
    public function truncate_table() {
        $sql = "TRUNCATE TABLE ekt_encomenda_item";
        return $this->db->query($sql);
    }
    
    
}