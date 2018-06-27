<?php

class Inventario_model extends CI_Model {

    public function __construct() {
        parent::__construct();
//         $this->load->database();
    }

    public function total_por_deptos() {

        $sql_total = "SELECT sum(preco_prazo) as total FROM vw_est_produto";
        $query = $this->db->query($sql_total);
        $r['total'] = $query->result_array()[0]['total'];

        $sql_por_deptos = "SELECT 
                        p.depto,
                        sum(if(p.qtde > 0 , p.preco_prazo * p.qtde, 0)) as total
                FROM
                        vw_est_produto p
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
