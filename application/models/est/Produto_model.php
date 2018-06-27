<?php
require_once ('application/models/dao/base/Base_model.php');

/**
 * Modelo para a tabela est_produto.
 */
class Produto_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct("est_produto");
    }

    public function findByReduzidoEkt($reduzidoEkt, $dtImportacao = null)
    {
        $sql = "SELECT * FROM est_produto WHERE reduzido_ekt = ? ";
        $params = array(
            $reduzidoEkt
        );
        
        if ($dtImportacao != null) {
            $sql .= "AND (reduzido_ekt_desde <= ? OR reduzido_ekt_desde IS NULL) AND (reduzido_ekt_ate >= ? OR reduzido_ekt_ate IS NULL) ";
            $params[] = $dtImportacao;
            $params[] = $dtImportacao;
        }
        $sql .= "ORDER BY reduzido_ekt_desde";
        
        $qry = $this->db->query($sql, $params);
        
        $r = $qry->result_array();
        
        if ($dtImportacao != null && count($r) > 1) {
            throw new Exception("Mais de um produto com o mesmo reduzido (" . $reduzidoEkt . ") no período (" . $dtImportacao . ")");
        } else {
            return $r;
        }
    }
}
