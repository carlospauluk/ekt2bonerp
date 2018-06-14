<?php 

class Ektvendaitem_model extends CI_Model {
    
    
    public function delete_by_mesano($mesano) {
        if (!$mesano) return false;
        $sql = "DELETE FROM ekt_venda_item WHERE mesano = ?";
        return $this->db->query($sql, array($mesano));
    }
    
    
}