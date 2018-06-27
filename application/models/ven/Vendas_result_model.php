<?php

class Vendas_result_model extends CI_Model {

    public function __construct() {
        parent::__construct();
//         $this->load->database();
    }

    public function total_por_deptos($mesano_desde, $mesano_ate) {

        $sql_total = "SELECT "
                . "sum( (vi.preco_venda * vi.qtde * v.valor_total / v.sub_total) ) as total FROM "
                . "ven_venda v JOIN ven_venda_item vi ON v.id = vi.venda_id "
                . "WHERE v.dt_venda BETWEEN ? AND ? AND "
                . "v.deletado IS FALSE AND "
                . "v.plano_pagto_id != 51";

        $desde = Datetime_utils::mesano2sqldate($mesano_desde);
        $ate = Datetime_utils::ultimo_dia(Datetime_utils::mesano2sqldate($mesano_ate));

        $query = $this->db->query($sql_total, array($desde, $ate));


        $r['total'] = $query->result_array()[0]['total'];

        $sql_por_deptos = "SELECT 
                        p.depto,
                        cast(round( sum(vi.preco_venda * vi.`qtde` * v.valor_total / v.sub_total) , 2) as decimal(18,2)) as total
                FROM
                        ven_venda v JOIN ven_venda_item vi ON v.id = vi.venda_id
                                LEFT JOIN `vw_est_produto` p ON vi.produto_id = p.id
                        WHERE 
                                v.dt_venda BETWEEN ? AND ? AND
                                v.`deletado` IS FALSE AND
                                v.`plano_pagto_id` != 51
                GROUP BY p.depto ORDER BY total DESC";

        $query = $this->db->query($sql_por_deptos, array($desde, $ate));
        $results = $query->result_array();

        for ($i=0 ; $i<count($results) ; $i++) {
            
            $porcent = $results[$i]['total'] / $r['total'] * 100.00;
            $porcent = number_format((float) $porcent, 2, '.', ''); 
            $results[$i]['porcent'] = $porcent;
            
        }
        
        $r['results'] = $results;

        return $r;
    }

}
