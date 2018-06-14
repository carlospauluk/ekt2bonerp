<?php

class Ektpedido_model extends CI_Model
{
    
    public function truncate_table() {
        $sql = "TRUNCATE TABLE ekt_pedido";
        return $this->db->query($sql);
    }
    

    public function findby_pedido($pedido)
    {
        $query = $this->db->get_where('ekt_pedido', array(
            'PEDIDO' => $pedido
        ));
        $result = $query->result_array();
        if (count($result) > 1) {
            throw new Exception('Mais de um registro com o mesmo id.');
        } else if (count($result) == 1) {
            return $result[0];
        } else {
            return null;
        }
    }
}