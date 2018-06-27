<?php
require_once ('application/models/dao/base/Base_model.php');

/**
 * Modelo para a tabela est_produto.
 */
class Ektfornecedor_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct("ekt_fornecedor", "ekt");
    }

    public function delete_by_mesano($mesano)
    {
        if (! $mesano)
            return false;
        $sql = "DELETE FROM ekt_fornecedor WHERE mesano = ?";
        return $this->db->query($sql, array(
            $mesano
        ));
    }
}
