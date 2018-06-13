<?php 

class Ektfornecedor_model extends CI_Model {
    
    
    public function delete_by_mesano($mesano) {
        if (!$mesano) return false;
        $sql = "DELETE FROM ekt_fornecedor WHERE mesano = ?";
        return $this->db->query($sql, array($mesano));
    }
    
    
}