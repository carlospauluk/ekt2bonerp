<?php

class Custos_venda_model extends CI_Model {

    public function __construct() {
        parent::__construct();
    }

    public function por_fornecedor($mesano_ini, $mesano_fim) {

        // seleciono todos os fornecedores que tiveram vendas no período
        $sql = "SELECT "
                . " distinct(p.fornecedor_id) "
                . "FROM ven_venda v, ven_venda_item vi, est_produto p "
                . "WHERE "
                . "v.id = vi.venda_id AND "
                . "vi.produto_id = p.id AND "
                . "v.dt_venda BETWEEN ? AND ?";
        $dt_ini = Datetime_utils::mesano2sqldate($mesano_ini);
        $dt_fim = Datetime_utils::ultimo_dia(Datetime_utils::mesano2sqldate($mesano_fim));
        $query = $this->db->query($sql, array($dt_ini, $dt_fim));
        $forns = $query->result_array();


        $sql_total = "
                    SELECT
                            SUM(vi.preco_custo * vi.qtde) as total_preco_custo,
                            SUM(vi.preco_venda * vi.qtde * v.valor_total / v.sub_total) as total_preco_venda,
                            round(SUM(vi.preco_custo * vi.qtde) / SUM(vi.preco_venda * vi.qtde * v.valor_total / v.sub_total) * 100 , 2) as cmv
                    FROM
                            ven_venda v, 
                            ven_venda_item vi
                    WHERE
                            v.id = vi.venda_id AND
                            vi.dt_custo IS NOT NULL AND
                            v.dt_venda BETWEEN ? AND ?
                    ";
        $qry_total = $this->db->query($sql_total, array($dt_ini, $dt_fim));
        $total = $qry_total->result_array()[0];
        
        $r['total_preco_custo'] = number_format((float)  $total['total_preco_custo'], 2, '.', '');
        $r['total_preco_venda'] = number_format((float)  $total['total_preco_venda'], 2, '.', '');
        $r['cmv'] = number_format((float)  $total['cmv'], 2, '.', '') . '%';


        $sql_subdeptos = "
                    SELECT
                            p.subdepto_id,
                            p.subdepto_codigo,
                            p.subdepto,
                            SUM(vi.preco_custo * vi.qtde) as total_preco_custo,
                            SUM(vi.preco_venda * vi.qtde * v.valor_total / v.sub_total) as total_preco_venda,
                            round(SUM(vi.preco_custo * vi.qtde) / SUM(vi.preco_venda * vi.qtde * v.valor_total / v.sub_total) * 100 , 2) as cmv
                    FROM
                            ven_venda v, 
                            ven_venda_item vi,
                            vw_est_produto p
                    WHERE
                            v.id = vi.venda_id AND
                            vi.produto_id = p.id AND
                            vi.dt_custo IS NOT NULL AND
                            v.dt_venda BETWEEN ? AND ? AND
                            p.fornecedor_id = ?
                    GROUP BY p.subdepto_id
                    ";
        
        
        $sql_total_forn = "
                    SELECT
                            SUM(vi.preco_custo * vi.qtde) as total_preco_custo,
                            SUM(vi.preco_venda * vi.qtde * v.valor_total / v.sub_total) as total_preco_venda,
                            round(SUM(vi.preco_custo * vi.qtde) / SUM(vi.preco_venda * vi.qtde * v.valor_total / v.sub_total) * 100 , 2) as cmv
                    FROM
                            ven_venda v, 
                            ven_venda_item vi,
                            vw_est_produto p
                    WHERE
                            v.id = vi.venda_id AND
                            vi.produto_id = p.id AND
                            vi.dt_custo IS NOT NULL AND
                            v.dt_venda BETWEEN ? AND ? AND
                            p.fornecedor_id = ?
                    ";


        $i = 0;
        foreach ($forns as $forn) {

            $fornecedor_id = $forn['fornecedor_id'];

            $query = $this->db->query("SELECT * FROM vw_est_fornecedor WHERE id = ?", $fornecedor_id);
            $fornecedor = $query->result_array()[0];
            $results[$i]['fornecedor'] = $fornecedor;
            
            $query = $this->db->query($sql_total_forn, array($dt_ini, $dt_fim, $fornecedor_id));
            $total_forn = $query->result_array()[0];
            
            $results[$i]['fornecedor']['total_preco_custo'] = number_format((float)  $total_forn['total_preco_custo'], 2, '.', '');
            $results[$i]['fornecedor']['total_preco_venda'] = number_format((float)  $total_forn['total_preco_venda'], 2, '.', '');
            $results[$i]['fornecedor']['cmv'] = number_format((float)  $total_forn['cmv'], 2, '.', '');
            

            $qry_subdeptos = $this->db->query($sql_subdeptos, array($dt_ini, $dt_fim, $fornecedor_id));
            $subdeptos = $qry_subdeptos->result_array();
            
                    
            $results[$i]['subdeptos'] = $subdeptos;
            
            $i++;
        }
        
        $r['results'] = $results;

        return $r;
    }

    public function por_subdepto($mesano_ini, $mesano_fim) {
        
    }

    public function por_depto($mesano_ini, $mesano_fim) {
        
    }

}
