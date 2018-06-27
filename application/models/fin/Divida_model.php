<?php

require_once('application/libraries/math/Math.php');

class Divida_model extends CI_Model {

    public function __construct() {
        parent::__construct();
//         $this->load->database();
    }

    public function listall_ddpcg($carteira = null) {
        $sql = "SELECT banco, descricao, dt_vencto, valor_total FROM vw_fin_ddpcg WHERE banco LIKE ? AND status = 'ABERTA' ORDER BY dt_vencto, valor_total";
        $query = $this->db->query($sql, '%' . $carteira . '%');
        return $query->result_array();
    }

    public function listagrup_ddpcg($carteira = null) {
        $sql = "SELECT banco, descricao, dt_vencto, sum(valor_total) as valor_total FROM vw_fin_ddpcg WHERE banco LIKE ? AND status = 'ABERTA' GROUP BY descricao ORDER BY descricao, valor_total";
        $query = $this->db->query($sql, '%' . $carteira . '%');
        return $query->result_array();
    }

    public function totalizar_ddpcg($carteira = null) {
        $sql = "SELECT sum(valor_total) as total FROM vw_fin_ddpcg WHERE banco LIKE ? AND status = 'ABERTA'";
        $query = $this->db->query($sql, '%' . $carteira . '%');
        $r = $query->result_array();
        if (count($r) > 0) {
            return $r[0]['total'];
        }
        return null;
    }

    /**
     * 
     * @param type $dt_base (formato YYYYmmdd)
     */
    public function listall_cgs($dt_base = null) {
        if ($dt_base == null) {
            $dt_base = date("Y-m-d");
        } else {
            $dt_base = DateTime::createFromFormat('Ymd', $dt_base)->format('Y-m-d');
        }
        // Pego todos os parcelamentos para obs = 'PGTOCG'
        $query = $this->db->query("SELECT distinct(parcelamento_id) FROM fin_movimentacao WHERE obs LIKE '%PGTOCG%' AND status = 'ABERTA'");
        $results = $query->result_array();
        
        $r['total_valor_parcelas'] = 0;
        $r['total_valor_emprestimos'] = 0;
        $r['total_valor_devedor_total'] = 0;
        $r['total_saldo_devedor'] = 0;
        $r['total_devedor_a_vista'] = 0;

        foreach ($results as $item) {
            $parcelamento_id = $item['parcelamento_id'];

            $sql = "SELECT id, descricao, dt_vencto, valor, cart_codigo, cart_descricao, count(*) as qtde_parcelas_total, obs FROM vw_fin_movimentacao WHERE parcelamento_id = ? GROUP BY parcelamento_id ORDER BY dt_vencto";
            $query = $this->db->query($sql, $parcelamento_id);
            $parcelamentos = $query->result_array();
            $parcelamento = $parcelamentos[0];
            
            $row['carteira'] = $parcelamento['cart_codigo'] . ' - ' . $parcelamento['cart_descricao'];
            $row['descricao'] = $parcelamento['descricao'];
            $row['primeira_parcela'] = $parcelamento['dt_vencto'];
            $row['valor_parcela'] = abs($parcelamento['valor']);
            $r['total_valor_parcelas'] += $row['valor_parcela'];

            $row['qtde_parcelas_total'] = $parcelamento['qtde_parcelas_total'];

            $row['valor_devedor_total'] = $parcelamento['valor'] * $row['qtde_parcelas_total'];
            
            $query = $this->db->query("SELECT count(*) as qtde_parcelas_restantes FROM vw_fin_movimentacao WHERE parcelamento_id = ? AND status = 'ABERTA' AND dt_vencto >= ?", array($parcelamento_id, $dt_base));
            $qtde_parcelas_restantes = $query->result_array()[0]['qtde_parcelas_restantes'];

            $row['qtde_parcelas_restantes'] = $qtde_parcelas_restantes;
            $row['saldo_devedor'] = floatval($row['qtde_parcelas_restantes']) * floatval($parcelamento['valor']);
            $r['total_saldo_devedor'] += $row['saldo_devedor'];
            
            // Na primeira parcela, na obs, tem uma informação no formato: VLR_ORIG=260000
            $pattern = '/(?P<label>VLR_ORIG={1})(?P<valor>\d+)/';
            preg_match($pattern, $parcelamento['obs'], $matches);
            
            $row['valor_emprestimo'] = number_format($matches['valor'], 2, '.', '');
            $r['total_valor_emprestimos'] += $row['valor_emprestimo'];
            
            $taxa = rate($row['qtde_parcelas_total'], $row['valor_parcela'], $row['valor_emprestimo']);
            $row['taxa'] = $taxa * 100;
            
            $row['devedor_a_vista'] = atualiza_divida($row['valor_parcela'], $taxa, $row['qtde_parcelas_restantes']);
            $r['total_devedor_a_vista'] += $row['devedor_a_vista'];
            
            $row['dif'];
            
            $r['itens'][] = $row;
        }

        
        
        return $r;
    }

}
