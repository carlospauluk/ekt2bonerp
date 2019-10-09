<?php

ini_set('max_execution_time', 0);
ini_set('memory_limit', '2048M');

class CorrigirVendas extends CI_Controller
{

    private $dbcrosier;

    public function __construct()
    {
        parent::__construct();
        $this->dbcrosier = $this->load->database('crosier', TRUE);
    }

    public function corrigir_custos_tudo()
    {
        $r = Datetime_utils::mesano_list('201611', '201711');
        foreach ($r as $mesano) {
            $this->corrigir_custos($mesano);
        }
    }

    /**
     *
     * @param type $mesano
     */
    public function corrigir_custos($mesano)
    {
        $time_start = microtime(true);

        echo "<pre>";
        $this->db->trans_start();

        $query = $this->db->query("SELECT vi.id, vi.produto_id, v.dt_venda FROM ven_venda v, ven_venda_item vi WHERE v.id = vi.venda_id AND v.mesano = ?", $mesano) or $this->exit_db_error();
        $result = $query->result_array();


        $i = 0;
        foreach ($result as $r) {
            try {

                if (!$r['produto_id']) {
                    continue;
                }

                $query = $this->db->query("SELECT dt_custo, preco_custo FROM est_produto_preco WHERE dt_custo <= ? AND produto_id = ? ORDER BY dt_custo", array($r['dt_venda'], $r['produto_id'])) or $this->exit_db_error();
                $produto = $query->result_array()[0];

                if (!$produto) {
                    $query = $this->db->query("SELECT reduzido_ekt FROM est_produto WHERE id = ?", $r['produto_id']) or $this->exit_db_error();
                    $produto = $query->result_array()[0];
                    if ($produto['reduzido_ekt'] == 88888) {
                        continue;
                    }
                    if (!$produto) {
                        $this->exit_db_error();
                    }

                    $query = $this->db->query("SELECT data_pcusto, pcusto FROM ekt_produto WHERE reduzido = ? and mesano = ?", array($produto['reduzido_ekt'], $mesano)) or $this->exit_db_error();
                    $ekt_produto = $query->result_array()[0];
                    if (!$ekt_produto) {
                        $this->exit_db_error();
                    }

                    $data['dt_custo'] = $ekt_produto['data_pcusto'];
                    $data['preco_custo'] = $ekt_produto['pcusto'];
                } else {

                    $data['dt_custo'] = $produto['dt_custo'];
                    $data['preco_custo'] = $produto['preco_custo'];
                }

                $this->db->update('ven_venda_item', $data, array('id' => $r['id'])) or $this->exit_db_error();
            } catch (Exception $e) {
                print_r($e->getMessage());
                exit;
            }
        }

        echo "\n\n\nTOTAL ATUALIZADO: " . $i . "\n";

        $this->db->trans_complete();


        $time_end = microtime(true);

        $execution_time = ($time_end - $time_start);

        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s";
    }

    private function exit_db_error()
    {
        echo str_pad("", 100, "*") . "\n";
        echo "LAST QUERY: " . $this->db->last_query() . "\n\n";
        print_r($this->db->error());
        echo str_pad("", 100, "*") . "\n";
        exit;
    }

    /**
     *
     * @param type $mesano
     */
    public function salvar_giro_estoque($mesano)
    {
        $time_start = microtime(true);

        echo "<pre>";
        $this->db->trans_start();

        $query = $this->db->query("SELECT "
            . "vi.id, vi.produto_id, v.dt_venda, p.fornecedor_id, p.subdepto_id, vi.grade_tamanho_id "
            . "FROM "
            . "ven_venda v, ven_venda_item vi, est_produto p "
            . "WHERE v.id = vi.venda_id AND "
            . "v.mesano = ? AND "
            . "vi.produto_id = p.id", $mesano) or $this->exit_db_error();
        $result = $query->result_array();


        $i = 0;
        foreach ($result as $r) {
            try {

                if (!$r['produto_id']) {
                    continue;
                }

                $query = $this->db->query("SELECT dt_custo, preco_custo FROM est_produto_preco WHERE dt_custo <= ? AND produto_id = ? ORDER BY dt_custo", array($r['dt_venda'], $r['produto_id'])) or $this->exit_db_error();
                $produto = $query->result_array()[0];

                if (!$produto) {
                    $query = $this->db->query("SELECT reduzido_ekt FROM est_produto WHERE id = ?", $r['produto_id']) or $this->exit_db_error();
                    $produto = $query->result_array()[0];
                    if ($produto['reduzido_ekt'] == 88888) {
                        continue;
                    }
                    if (!$produto) {
                        $this->exit_db_error();
                    }

                    $query = $this->db->query("SELECT data_pcusto, pcusto FROM ekt_produto WHERE reduzido = ? and mesano = ?", array($produto['reduzido_ekt'], $mesano)) or $this->exit_db_error();
                    $ekt_produto = $query->result_array()[0];
                    if (!$ekt_produto) {
                        $this->exit_db_error();
                    }

                    $data['dt_custo'] = $ekt_produto['data_pcusto'];
                    $data['preco_custo'] = $ekt_produto['pcusto'];
                } else {

                    $data['dt_custo'] = $produto['dt_custo'];
                    $data['preco_custo'] = $produto['preco_custo'];
                }

                $this->db->update('ven_venda_item', $data, array('id' => $r['id'])) or $this->exit_db_error();
            } catch (Exception $e) {
                print_r($e->getMessage());
                exit;
            }
        }

        echo "\n\n\nTOTAL ATUALIZADO: " . $i . "\n";

        $this->db->trans_complete();


        $time_end = microtime(true);

        $execution_time = ($time_end - $time_start);

        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s";
    }

    public function cadeiaUnqcs()
    {

        $query = $this->dbcrosier->query("SELECT id FROM fin_cadeia WHERE unqc IS NULL") or $this->exit_db_error();
        $result = $query->result_array();

        $i = 0;
        foreach ($result as $r) {
            echo "Atualizando $i\n";
            $data['unqc'] = md5(uniqid(rand(), true));
            $this->dbcrosier->update('fin_cadeia', $data, array('id' => $r['id'])) or $this->exit_db_error();
        }
        echo "\n\n\nOK";

    }

}
