<?php

class Ger_precos_uniformes_model extends CI_Model {

    public function __construct() {
        parent::__construct();
//         $this->load->database();
    }

    public function get_escolas() {
        $query = $this->db->query("SELECT id, codigo_ekt, nome_fantasia FROM vw_est_fornecedor WHERE codigo_ekt BETWEEN 501 AND 608 ORDER BY nome_fantasia") or $this->exit_db_error();
        return $query->result_array();
    }

    public function get_subdeptos($escolas_ids) {
        $escolas_ids = explode('-', $escolas_ids);
        $query = $this->db->query("SELECT "
                . "s.id, s.nome as subdepto "
                . "FROM est_produto p, est_subdepto s "
                . "WHERE p.subdepto_id = s.id AND p.fornecedor_id IN ? "
                . "GROUP BY p.subdepto_id", array($escolas_ids)) or $this->exit_db_error();
        return $query->result_array();
    }

    public function get_tamanhos() {
        $query = $this->db->query("SELECT id, tamanho FROM est_grade_tamanho WHERE grade_id IN (1,3) ORDER BY grade_id DESC, ordem") or $this->exit_db_error();
        return $query->result_array();
    }

    public function get_produtos($escola_id, $subdeptos_ids, $tamanho_id) {

        $query = $this->db->query("SELECT "
                . "p.id, p.reduzido_ekt, p.descricao, p.preco_prazo, p.preco_vista, p.preco_promo, gt.tamanho, s.qtde "
                . "FROM "
                . "vw_est_produto p, est_produto_saldo s, est_grade_tamanho gt "
                . "WHERE "
                . "s.produto_id = p.id AND "
                . "s.grade_tamanho_id = gt.id AND "
                . "fornecedor_id = ? AND "
                . "p.subdepto_id IN ? AND "
                . "gt.id = ? AND "
                . "s.selec IS TRUE", array($escola_id, $subdeptos_ids, $tamanho_id)) or $this->exit_db_error();
        return $query->result_array();
    }


}
