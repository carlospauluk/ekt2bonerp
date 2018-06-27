<?php

class Ektencomenda_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct("ekt_encomenda", "ekt");
    }

    public function truncate_table()
    {
        $sql = "TRUNCATE TABLE ekt_encomenda";
        return $this->db->query($sql);
    }
}