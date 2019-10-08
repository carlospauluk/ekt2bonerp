<?php
require_once('application/models/dao/base/Base_model.php');

/**
 * Modelo para a tabela est_produto.
 */
class Produto_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct("est_produto", "crosier");
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

    public function findByReduzidoEktAndMesano($reduzidoEkt, $mesano)
    {
        $sql = "SELECT p.* FROM est_produto_reduzidoektmesano r, est_produto p WHERE p.id = r.produto_id AND r.reduzido_ekt = ? AND r.mesano = ?";

        $params = array(
            $reduzidoEkt,
            $mesano
        );

        $r = $this->db->query($sql, $params)->result_array();

        if (count($r) == 1) {
            return $r[0];
        } else if (count($r) == 0) {
            return null;
        } else {
            throw new \Exception("Mais de um produto encontrado na est_produto_reduzidoektmesano para reduzido_ekt = [" . $reduzidoEkt . "] e mesano = [" . $mesano . "]");
        }
    }

    public function findProdutosLojaVirtual()
    {
        $sql = "SELECT p.id, p.reduzido, p.reduzido_ekt FROM est_produto p, est_produto_oc_product ocp WHERE p.id = ocp.est_produto_id AND p.atual = true";
        $qry = $this->db->query($sql);
        $r = $qry->result_array();
        return $r;
    }

}
