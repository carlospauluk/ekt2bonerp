<?php

class Ektencomendaitem_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct("ekt_encomenda_item", "ekt");
    }

    public function truncate_table()
    {
        $sql = "TRUNCATE TABLE ekt_encomenda_item";
        return $this->db->query($sql);
    }
}