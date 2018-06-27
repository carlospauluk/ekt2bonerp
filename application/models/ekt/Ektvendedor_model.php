<?php

class Ektvendedor_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct("ekt_vendedor", "ekt");
    }

    public function truncate_table()
    {
        $sql = "TRUNCATE TABLE ekt_vendedor";
        return $this->db->query($sql);
    }
}