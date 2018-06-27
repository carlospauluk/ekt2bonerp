<?php

class Ektproduto_model extends CI_Model
{

    public function findByMesano($mesano)
    {
        $sql = "SELECT * FROM ekt_produto WHERE mesano = ?";
        $query = $this->db->query($sql, array(
            $mesano
        ));
        return $query->result_array();
    }

    public function delete_by_mesano($mesano)
    {
        if (! $mesano)
            return false;
        $sql = "DELETE FROM ekt_produto WHERE mesano = ?";
        return $this->db->query($sql, array(
            $mesano
        ));
    }
}