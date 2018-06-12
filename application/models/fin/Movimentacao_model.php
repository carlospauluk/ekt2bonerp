<?php

require_once('application/models/base/Base_model.php');

/**
 * Modelo para a tabela fin_movimentacao
 */
class Movimentacao_model extends Base_model {

    public function __construct() {
        parent::__construct("fin_movimentacao");
    }

    public function load_formatters() {
        
    }

    public function find_apagar($mesano) {
        $sql = "SELECT categ_codigo, categ_descricao, SUM(valor_total) as valor_total FROM vw_fin_movimentacao WHERE mes_ano = ? AND status != 'REALIZADA' AND valor_total < 0 AND not cart_cartoes GROUP BY categ_codigo ORDER BY SUM(valor_total)";
        $query = $this->db->query($sql, array($mesano));
        $result = $query->result_array();
        return $result;
    }

    public function listby($mesano, $codigo) {
        $sql = "SELECT * FROM vw_fin_movimentacao  "
                        . "WHERE categ_codigo LIKE ? AND "
                        . "DATE_FORMAT(dt_util,'%Y%m') = ?";
        
        $query = $this->db->query($sql, array($codigo . "%", $mesano));
        $result = $query->result_array();
        return $result;
    }

}
