<?php

class ImportarVendas extends CI_Controller {

    public function __construct() {
        parent::__construct();
        $this->load->database();
    }

    /**
     * Corrigindo registros que estavam marcados como 9.99, sendo que deveriam ser 0.99 (cancelados)
     * @param type $mesano
     */
    public function corrigir_planos_pagto() {
        $time_start = microtime(true);

        echo "<pre>";
        $this->db->trans_start();

        $query = $this->db->query("SELECT * FROM ekt_venda WHERE cond_pag = '0.99'") or exit_db_error();
        $result = $query->result_array();

        // Pega todos os produtos da ekt_produto para o $mesano
        $i = 0;
        foreach ($result as $r) {
            try {
                $query = $this->db->query("SELECT * FROM ven_venda WHERE pv = ? AND mesano = ?", array($r['NUMERO'], $r['mesano'])) or exit_db_error();
                $naVenVenda = $query->result_array();
                if (count($naVenVenda) == 0) {
                    echo "Não encontrado para pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'\n";
                } else if (count($naVenVenda) == 1) {
                    if ($naVenVenda[0]['plano_pagto_id'] == 2) {
                        $i++;
                        echo "Atualizando o pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'\n";
                        $this->db->query("UPDATE ven_venda SET plano_pagto_id = 158 WHERE id = ?", $naVenVenda[0]['id']) or exit_db_error();
                    }
                } else {
                    echo "Mais de um encontrado para pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'\n";
                    exit;
                }
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

    private function exit_db_error() {
        echo str_pad("", 100, "*") . "\n";
        echo "LAST QUERY: " . $this->db->last_query() . "\n\n";
        print_r($this->db->error());
        echo str_pad("", 100, "*") . "\n";
        exit;
    }

    public function teste($mesano) {
        $results = $this->findByReduzidoEkt(1234, '201702');
        print_r($results);
    }

}
