<?php
require_once('application/models/dao/base/Base_model.php');

/**
 * Modelo para a tabela est_fornecedor.
 */
class Fornecedor_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct('est_fornecedor', 'crosier');
    }

    /**
     * @param $codigo
     * @param $mesano
     * @return mixed
     * @throws Exception
     */
    public function findByCodigoEkt($codigo, $mesano)
    {
        $sql = 'SELECT fornecedor_id FROM est_fornecedor_codektmesano WHERE codigo_ekt = ? AND mesano = ?';

        $params = [
            $codigo,
            $mesano
        ];

        $qry = $this->db->query($sql, $params);

        $rs = $qry->result_array();

        if (count($rs) < 1) {
            throw new \Exception('Nenhum fornecedor encontrado. Código: ' . $codigo);
        } else {
            if (count($rs) > 1) {
                throw new \Exception('Mais de um fornecedor encontrado. Código: ' . $codigo);
            } else {
                return $rs[0]['fornecedor_id'];
            }
        }
    }

}
