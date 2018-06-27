<?php

class Ektvenda_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct("ekt_venda", "ekt");
    }

    public function delete_by_mesano($mesano)
    {
        if (! $mesano)
            return false;
        $sql = "DELETE FROM ekt_venda WHERE mesano = ?";
        return $this->db->query($sql, array(
            $mesano
        ));
    }
}