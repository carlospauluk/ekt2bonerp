<?php

class CorrigirPrecos extends CI_Controller {

    private $inseridos;
    private $existentes;

    public function __construct() {
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        parent::__construct();
        $this->load->database();
    }

    public function corrigeTudo($mesano_ini, $mesano_fim) {
        $l = Datetime_utils::mesano_list($mesano_ini, $mesano_fim);
        foreach ($l as $mesano) {
            $this->corrigeProdutoPreco($mesano);
        }
    }

    /**
     * Tava com muitos errados na tabela est_produto_preco.
     * Dei um TRUNCATE nela e fiz este método para ajustar tudo.
     * @param type $mesano
     */
    public function corrigeProdutoPreco($mesano) {
        $time_start = microtime(true);

        echo "<hr>";
        echo "Corrigindo para " . $mesano;
        echo "<hr>";


        $this->db->trans_start();

        $query = $this->db->get_where("ekt_produto", array('mesano' => $mesano));
        $result = $query->result_array();

        // Pega todos os produtos da ekt_produto para o $mesano
        $i = 1;
        foreach ($result as $r) {
            try {
                // Para cada ekt_produto, encontra o est_produto
                $produto = $this->findByReduzidoEkt($r['REDUZIDO'], $mesano)[0];
                // Adiciona o preço
                echo $i++ . " (" . $r['id'] . ")<br />";
                $this->salvarProdutoPreco($r, $produto['id'], $mesano);
            } catch (Exception $e) {
                print_r($e->getMessage());
                exit;
            }
        }

        $this->db->trans_complete();

        echo "\n\n\nINSERIDOS: " . $this->inseridos;
        echo "\nEXISTENTES: " . $this->existentes;

        echo "<br />";
        echo "<br />";
        echo "<br />";

        $time_end = microtime(true);

        $execution_time = ($time_end - $time_start);

//execution time of the script
        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s";
    }

    public function salvarProdutoPreco($produtoEkt, $produtoId, $mesano) {

        if (!$produtoEkt['DATA_PCUSTO']) {
            $produtoEkt['DATA_PCUSTO'] = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
        }

        if (!$produtoEkt['DATA_PVENDA']) {
            $produtoEkt['DATA_PVENDA'] = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
        }

        $query = $this->db->get_where('est_produto_preco', array(
            'produto_id' => $produtoId,
            'dt_custo' => $produtoEkt['DATA_PCUSTO'],
            'preco_custo' => $produtoEkt['PCUSTO'],
            'preco_prazo' => $produtoEkt['PPRAZO']
                )
        );
        $existe = $query->result_array();
//        echo "COMANDO: " . $this->db->last_query() . "\n";
//        print_r($existe);
//        echo "\n\nCONT: " . count($existe) . "\n";
//        exit;
        if (count($existe) > 0) {
            $this->existentes++;
            return;
        }

        $mesano_dt = Datetime_utils::mesano2sqldate($mesano);

        $data = array(
            'inserted' => date("Y-m-d H:i:s"),
            'updated' => date("Y-m-d H:i:s"),
            'version' => 0,
            'coeficiente' => $produtoEkt['COEF'],
            'custo_operacional' => $produtoEkt['MARGEMC'],
            'dt_custo' => $produtoEkt['DATA_PCUSTO'],
            'dt_preco_venda' => $produtoEkt['DATA_PVENDA'],
            'margem' => $produtoEkt['MARGEM'],
            'prazo' => $produtoEkt['PRAZO'],
            'preco_custo' => $produtoEkt['PCUSTO'],
            'preco_prazo' => $produtoEkt['PPRAZO'],
            'preco_promo' => $produtoEkt['PPROMO'],
            'preco_vista' => $produtoEkt['PVISTA'],
            'estabelecimento_id' => 1,
            'user_inserted_id' => 1,
            'user_updated_id' => 1,
            'produto_id' => $produtoId,
            'custo_financeiro' => 0.15,
            'mesano' => $mesano_dt
        );

        $r = $this->db->insert('est_produto_preco', $data);

        $err = $this->db->error();
        if ($err['code']) {
            $erro = "Erro ao executar comando no BD.\n"
                    . "COMANDO: " . $this->db->last_query() . "\n"
                    . "ERRO: " . $err['code'] . " (" . $err['message'] . ")";
            throw new Exception($erro);
        } else {
            $this->inseridos++;
            $r;
        }
    }

    public function findProdutosComMesmoReduzidoEKT($reduzidoEkt, $mesano) {


        $results = $this->findByReduzidoEkt($reduzidoEkt, $mesano);

        foreach ($results as $produto) {
            
        }

//
//        for (Produto mesmoReduzido : produtosComMesmoReduzidoEKT) {
//            if (ektProduto . equalsTo(mesmoReduzido) ||
//                    (mesmoReduzido . getReduzidoEktAte() == null && mesmoReduzido . getReduzidoEktDesde() . equals(dtMesImport))) {
//                logger . info("PRODUTO JÁ EXISTENTE");
//                achouMesmo = true;
//                mesmoReduzido = getThiz() . atualizaProduto(ektProduto, mesmoReduzido);
//
//                mesmoReduzido = getThiz() . salvarGrade(ektProduto, mesmoReduzido);
//
//                a++;
//                logger . info("QTDE NO EKT: " + ektProduto . getQtdeTotal());
//                logger . info("updateQtdeTotal(mesmoReduzido = '" + mesmoReduzido + "');");
//                mesmoReduzido = getProdutoBusiness() . updateQtdeTotal(mesmoReduzido);
//                logger . info("QTDE SALVA: " + mesmoReduzido . getQtdeTotal());
//                if (ektProduto . getQtdeTotal() . doubleValue() != mesmoReduzido . getQtdeTotal() . doubleValue()) {
//                    logger . info("************ ATENÇÃO: qtdes divergindo");
//                }
//                getThiz() . acertaPeriodosReduzidoEKT(ektProduto . getReduzido() . intValue());
//                break;
//            }
//        }
    }

    /**
     * 
     * 
     * 
     * @param type $reduzidoEkt
     * @param type $mesano
     * @return type
     * @throws ViewException
     */
    public function findByReduzidoEkt($reduzidoEkt, $mesano = null) {

        $params = array();


        $sql = "SELECT id FROM est_produto WHERE reduzido_ekt = ? ";
        $params[] = $reduzidoEkt;

        if ($mesano) {
            $sql .= "AND (reduzido_ekt_desde <= ? OR reduzido_ekt_desde IS NULL) "
                    . "AND (reduzido_ekt_ate >= ? OR reduzido_ekt_ate IS NULL) ";
            $params[] = DateTime::createFromFormat('Ym', $mesano)->format('Y-m-d');
            $params[] = DateTime::createFromFormat('Ym', $mesano)->format('Y-m-d');
        }

        $sql .= " ORDER BY reduzido_ekt_desde";

        $query = $this->db->query($sql, $params);
        $result = $query->result_array();


        if ($mesano && count($result) > 1) {
            throw new Exception("Mais de um produto com o mesmo reduzido (" . $reduzidoEkt . ") no período ('$mesano')");
        } else {
            return $result;
        }
    }

    public function teste($mesano) {
        $results = $this->findByReduzidoEkt(1234, '201702');
        print_r($results);
    }

}
