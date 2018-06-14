<?php 

class Ektvendedor_model extends CI_Model {
    
    
    public function truncate_table() {
        $sql = "TRUNCATE TABLE ekt_vendedor";
        return $this->db->query($sql);
    }
    
    
}