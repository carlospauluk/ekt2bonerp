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

    /**
     * Encontra todos os produtos com o reduzido (ekt) passado.
     *
     * @param $reduzidoEkt
     * @return mixed
     */
    public function findByReduzidoEkt($reduzidoEkt)
    {
        $sql = 'SELECT * FROM est_produto WHERE json_data->>"$.reduzido" = ? ORDER BY inserted';
        $params = [
            $reduzidoEkt
        ];
        /** @var CI_DB_mysqli_result $qry */
        $qry = $this->db->query($sql, $params);
        $r = $qry->result_array();
        return $r;
    }

    /**
     *
     * @param $reduzidoEkt
     * @param $mesano
     * @return |null
     * @throws Exception
     */
    public function findByReduzidoEktAndMesano($reduzidoEkt, $mesano)
    {
        $sql = "SELECT p.* FROM est_produto_reduzidoektmesano r, est_produto p WHERE p.id = r.produto_id AND r.reduzido_ekt = ? AND r.mesano = ?";

        $params = [
            $reduzidoEkt,
            $mesano
        ];

        $r = $this->db->query($sql, $params)->result_array();

        if (count($r) == 1) {
            return $r[0];
        } else if (count($r) == 0) {
            return null;
        } else {
            throw new \Exception("Mais de um produto encontrado na est_produto_reduzidoektmesano para reduzido ekt = [" . $reduzidoEkt . "] e mesano = [" . $mesano . "]");
        }
    }


}
