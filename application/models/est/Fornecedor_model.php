<?php
require_once ('application/models/dao/base/Base_model.php');

/**
 * Modelo para a tabela est_fornecedor.
 */
class Fornecedor_model extends CIBases\Models\DAO\Base\Base_model
{

    public function __construct()
    {
        parent::__construct("est_fornecedor", "bonerp");
    }

    public function findByCodigoEkt($codigo, \DateTime $dtMesano)
    {
        
        
        $ini = $dtMesano->format('Y-m-') . "01";
        $fim = $dtMesano->format('Y-m-t');
        
        $sql = "SELECT id FROM est_fornecedor WHERE codigo_ekt = ? AND codigo_ekt_desde <= ? AND (codigo_ekt_ate IS NULL OR codigo_ekt_ate >= ?)";
        
        $params = array(
            $codigo,
            $ini,
            $fim
        );
        
        $qry = $this->db->query($sql, $params);
        
        
        $rs = $qry->result_array();
        
        if (count($rs) < 1) {
            throw new \Exception("Nenhum fornecedor encontrado. Código: " . $codigo);
        } else {
            if (count($rs) > 1) {
                throw new \Exception("Mais de um fornecedor encontrado. Código: " . $codigo);
            } else {
                return $rs[0]['id'];
            }
        }
    }
}
