<?php

class ImportarVendas extends CI_Controller
{

    private $agora;

    /**
     * Qual a pasta dos CSVs.
     * Será obtido a partir da variável de ambiente EKT_CSVS_PATH.
     *
     * @var string
     */
    private $csvsPath;

    /**
     * Qual a pasta do log.
     * Será obtido a partir da variável de ambiente EKT_LOG_PATH.
     *
     * @var string
     */
    private $logPath;

    /**
     * Passado pela linha de comando no formato YYYYMM.
     *
     * @var string
     */
    private $mesAno;

    /**
     * Parseado do $mesAno para um DateTime.
     *
     * @var DateTime
     */
    private $dtMesano;

    private $inseridas;

    private $atualizadas;

    /**
     * Conexão ao db ekt.
     */
    private $dbekt;

    /**
     * Conexão ao db bonerp.
     */
    private $dbbonerp;

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        
        $this->dbekt = $this->load->database('ekt', TRUE);
        $this->dbbonerp = $this->load->database('bonerp', TRUE);
        
        $this->load->library('datetime_library');
        error_reporting(E_ALL);
        ini_set('display_errors', 1);
        
        $this->agora = new DateTime();
        
        $this->load->model('est/produto_model');
        $this->produto_model->setDb($this->dbbonerp);
        
        $this->venda_model = new \CIBases\Models\DAO\Base\Base_model('ven_venda', 'bonerp');
        $this->venda_model->setDb($this->dbbonerp);
        
        $this->vendaitem_model = new \CIBases\Models\DAO\Base\Base_model('ven_venda_item', 'bonerp');
        $this->vendaitem_model->setDb($this->dbbonerp);
        
        $this->ektvenda_model = new \CIBases\Models\DAO\Base\Base_model('ekt_venda', 'ekt');
        $this->ektvenda_model->setDb($this->dbekt);
    }

    /**
     * Método principal.
     *
     * @param $mesano (yyyymm)
     * @param $acao (PROD,
     *            DEATE)
     */
    public function importar($mesano = null)
    {
        $time_start = microtime(true);
        
        echo PHP_EOL . PHP_EOL;
        
        $this->csvsPath = getenv('EKT_CSVS_PATH') or die("EKT_CSVS_PATH não informado" . PHP_EOL . PHP_EOL . PHP_EOL);
        $this->logPath = getenv('EKT_LOG_PATH') or die("EKT_LOG_PATH não informado" . PHP_EOL . PHP_EOL . PHP_EOL);
        
        echo "csvsPath: [" . $this->csvsPath . "]" . PHP_EOL;
        echo "logPath: [" . $this->logPath . "]" . PHP_EOL;
        $this->logFile = $this->logPath . "espelhos2bonerp-PROD-" . $this->agora->format('Y-m-d_H-i-s') . ".txt";
        echo "logFile: [" . $this->logFile . "]" . PHP_EOL;
        
        echo "Iniciando a importação para o mês/ano: [" . $mesano . "]" . PHP_EOL;
        $this->mesano = $mesano;
        $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01");
        if (! $this->dtMesano instanceof DateTime) {
            die("mesano inválido." . PHP_EOL . PHP_EOL . PHP_EOL);
        }
        $this->dtMesano->setTime(0, 0, 0, 0);
        echo "OK!!!" . PHP_EOL . PHP_EOL;
        
        $this->importarVendas();
        
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "INSERIDAS: " . $this->inseridas . PHP_EOL;
        echo "ATUALIZADAS: " . $this->atualizadas . PHP_EOL;
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "----------------------------------" . PHP_EOL;
        echo "Total Execution Time: " . $execution_time . "s" . PHP_EOL . PHP_EOL . PHP_EOL;
    }

    public function importarVendas()
    {
        echo "Iniciando a importação de vendas..." . PHP_EOL;
        $this->dbbonerp->trans_start();
        
        $l = $this->dbekt->query("SELECT * FROM ekt_venda WHERE mesano = ?", array(
            $this->mesano
        ))->result_array();
        
        // $l = $this->dbekt->query("SELECT * FROM ekt_produto WHERE reduzido = 4521 AND mesano = ?", array($this->mesano))->result_array();
        
        $total = count($l);
        echo " >>>>>>>>>>>>>>>>>>>> " . $total . " venda(s) encontrada(s)." . PHP_EOL;
        
        $i = 0;
        foreach ($l as $ektVenda) {
            echo " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total . PHP_EOL;
            $this->importarVenda($ektVenda);
        }
        
        echo "Finalizando... commitando a transação..." . PHP_EOL;
        
        $this->dbbonerp->trans_complete();
        
        echo "OK!!!" . PHP_EOL;
    }

    public function importarVenda($ektVenda)
    {
        $params = array(
            $ektVenda['NUMERO'],
            $ektVenda['EMISSAO']
        );
        
        $vendas = $this->dbbonerp->query("SELECT * FROM ven_venda WHERE pv = ? AND dt_venda = ?", $params)->result_array();
        $q = count($vendas);
        
        if ($q > 1) {
            echo "Mais de uma venda encontrada para os dados" . PHP_EOL;
            print_r($params);
            exit();
        }
        
        $venda = null;
        if ($q == 1) {
            $venda = $vendas[0];
            $this->deletarItens($venda);
        }
        
        $this->salvarVenda($ektVenda, $venda);
    }

    public function deletarItens($venda)
    {
        $this->dbbonerp->query("DELETE FROM ven_venda_item WHERE venda_id = ?", array(
            $venda['id']
        )) or die("Erro ao deletar itens da venda_id = [" . $venda['id'] . "]" . PHP_EOL);
    }

    public function salvarVenda($ektVenda, $venda = null)
    {
        $venda['status'] = 'FINALIZADA';
        $venda['tipo_venda_id'] = 1;
        $venda['mesano'] = $this->mesano;
        $venda['pv'] = $ektVenda['NUMERO'];
        $venda['dt_venda'] = $ektVenda['EMISSAO'];
        
        $venda['plano_pagto_id'] = $this->findPlanoPagto($ektVenda['COND_PAG']);
        
        $venda['vendedor_id'] = $this->findVendedor($ektVenda['VENDEDOR']);
        
        $params = array(
            $this->mesano,
            $ektVenda['NUMERO']
        );
        $ektItens = $this->dbekt->query("SELECT * FROM ekt_venda_item WHERE mesano = ? AND NUMERO_NF = ?", $params)->result_array();
        
        // Para cada item da venda...
        $subTotalVenda = 0.0;
        $venda_itens = array();
        foreach ($ektItens as $ektItem) {
            echo "SALVANDO ITEM: " . $ektItem['PRODUTO'] . " - [" . $ektItem['DESCRICAO'] . "]" . PHP_EOL;
            
            $ektProduto = $this->dbekt->query("SELECT * from ekt_produto WHERE REDUZIDO = ? AND mesano = ?", array(
                $ektItem['PRODUTO'],
                $this->mesano
            ))->result_array() or die("ekt_produto não encontrado para REDUZIDO = [" . $ektItem['PRODUTO'] . "] e mesano = [" . $this->mesano . "]");
            if (count($ektProduto) != 1) {
                die("mais de 1 ekt_produto encontrado para REDUZIDO = [" . $ektItem['PRODUTO'] . "] e mesano = [" . $this->mesano . "]");
            }
            $ektProduto = $ektProduto[0];
            
            $itemVenda = array();
            
            $itemVenda['qtde'] = $ektItem['QTDE'];
            $itemVenda['preco_venda'] = $ektItem['VLR_UNIT'];
            
            $valorTotal = round($itemVenda['qtde'] * $itemVenda['preco_venda'], 2);
            
            if ($valorTotal != $ektItem['VLR_TOTAL']) {
                die("********** ATENÇÃO: erro em total de produto. Total Produto EKT: " . $valorTotal . ". Total Calculado: " . $ektItem['VLR_TOTAL']);
            }
            
            $itemVenda['nc_descricao'] = $ektItem['DESCRICAO'];
            $itemVenda['nc_reduzido'] = $ektItem['PRODUTO'];
            $itemVenda['nc_grade_tamanho'] = $ektItem['TAMANHO'];
            
            // Para NCs
            if ($ektItem['PRODUTO'] == 88888) {
                $itemVenda['obs'] = "NC 88888";
            } else {
                
                $produto = $this->produto_model->findByReduzidoEktAndMesano($ektItem['PRODUTO'], $this->mesano);
                if (!$produto) {
                    die("est_produto não encontrado para REDUZIDO = [" . $itemVenda['PRODUTO'] . "] em mesano = [" . $this->mesano . "]" . PHP_EOL . PHP_EOL);
                }
                $itemVenda['produto_id'] = $produto['id'];
                $itemVenda['grade_tamanho_id'] = $this->findGradeTamanhoByCodigoAndTamanho($ektProduto['GRADE'], $ektItem['TAMANHO']);
                
                $params = array(
                    $produto['id'],
                    $itemVenda['preco_venda'],
                    $itemVenda['preco_venda']
                );
                
                $precos = $this->dbbonerp->query("SELECT * FROM est_produto_preco WHERE produto_id = ? AND (preco_prazo = ? OR preco_promo = ?)", $params)->result_array();
                
                if (count($precos) > 0) {
                    $itemVenda['alteracao_preco'] = false;
                } else {
                    $itemVenda['alteracao_preco'] = true;
                }
                
                $itemVenda['ncm'] = $produto['ncm'];
            }
            $subTotalVenda += $valorTotal;
            
            $venda_itens[] = $itemVenda;
        }
        
        $venda['desconto_plano'] = $ektVenda['DESC_ACRES'] ? $ektVenda['DESC_ACRES'] : 0.0;
        $venda['desconto_especial'] = $ektVenda['DESC_ESPECIAL'] ? $ektVenda['DESC_ESPECIAL'] : 0.0;
        $venda['historicoDesconto'] = $ektVenda['HIST_DESC'];
        
        if ($subTotalVenda != $ektVenda['SUB_TOTAL']) {
            die("********** ATENÇÃO: erro em SUB TOTAL VENDA: " . $ektVenda['SUB_TOTAL'] . ". TOTAL SOMADO: " . $subTotalVenda . PHP_EOL);
        }
        
        $venda['sub_total'] = $subTotalVenda;
        
        $totalVenda = $subTotalVenda + $venda['desconto_plano'] + $venda['desconto_especial'];
        
        if ($totalVenda != $ektVenda['TOTAL']) {
            die("********** ATENÇÃO: erro em TOTAL VENDA: " . $ektVenda['TOTAL'] . ". TOTAL SOMA: " . $totalVenda . PHP_EOL);
        }
        
        $venda['valor_total'] = $totalVenda;
        
        $venda_id = $this->venda_model->save($venda);
        
        foreach ($venda_itens as $venda_item) {
            $venda_item['venda_id'] = $venda_id;
            $this->vendaitem_model->save($venda_item);
        }
        
        echo ">>>>>>>>>>>>>>>> VENDA SALVA" . PHP_EOL;
    }

    /*
     * Corrigindo registros que estavam marcados como 9.99, sendo que deveriam ser 0.99 (cancelados)
     *
     * @param
     * $mesano
     */
    public function corrigir_planos_pagto()
    {
        $time_start = microtime(true);
        
        echo "<pre>";
        $this->db->trans_start();
        
        $query = $this->db->query("SELECT * FROM ekt_venda WHERE cond_pag = '0.99'") or exit_db_error();
        $result = $query->result_array();
        
        // Pega todos os produtos da ekt_produto para o $mesano
        $i = 0;
        foreach ($result as $r) {
            try {
                $query = $this->db->query("SELECT * FROM ven_venda WHERE pv = ? AND mesano = ?", array(
                    $r['NUMERO'],
                    $r['mesano']
                )) or exit_db_error();
                $naVenVenda = $query->result_array();
                if (count($naVenVenda) == 0) {
                    echo "Não encontrado para pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'\n";
                } else if (count($naVenVenda) == 1) {
                    if ($naVenVenda[0]['plano_pagto_id'] == 2) {
                        $i ++;
                        echo "Atualizando o pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'\n";
                        $this->db->query("UPDATE ven_venda SET plano_pagto_id = 158 WHERE id = ?", $naVenVenda[0]['id']) or exit_db_error();
                    }
                } else {
                    echo "Mais de um encontrado para pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'\n";
                    exit();
                }
            } catch (Exception $e) {
                print_r($e->getMessage());
                exit();
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
        exit();
    }

    public function teste($mesano)
    {
        $results = $this->findByReduzidoEkt(1234, '201702');
        print_r($results);
    }

    private $planosPagto;

    public function findPlanoPagto($condPag)
    {
        if (! $this->planosPagto) {
            $r = $this->dbbonerp->query("SELECT id, codigo, descricao FROM ven_plano_pagto WHERE codigo = ?", array(
                $condPag
            ))->result_array();
            if (! $r or count($r) < 1) {
                die("Nenhum plano de pagto encontrado na base.");
            }
            foreach ($r as $pp) {
                $this->planosPagto[$pp['codigo']] = $pp['id'];
            }
        }
        if (! $this->planosPagto[$condPag]) {
            die("Plano de pagto não encontrado para: [" . $condPag . "]" . PHP_EOL . PHP_EOL);
        }
        return $this->planosPagto[$condPag];
    }

    private $vendedores;

    private $vendedores_nomes;

    public function findVendedor($codigo)
    {
        if (! $this->vendedores_nomes) {
            $nomes = $this->dbekt->query("SELECT CODIGO, DESCRICAO FROM ekt_vendedor WHERE mesano = ?", array(
                $this->mesano
            ))->result_array();
            if (count($nomes) < 1) {
                die("Erro ao pesquisar nome do vendedor. CODIGO = [" . $codigo . "]" . PHP_EOL);
            }
            
            foreach ($nomes as $nome) {
                $this->vendedores_nomes[$nome['CODIGO']] = trim($nome['DESCRICAO']);
            }
        }
        
        if (! $this->vendedores) {
            
            $r = $this->dbbonerp->query("SELECT id, codigo, nome_ekt FROM rh_funcionario")->result_array();
            if (count($r) < 1) {
                die("Nenhum vendedor encontrado na base.");
            }
            foreach ($r as $v) {
                $nome_ekt = trim($v['nome_ekt']);
                $this->vendedores[$v['codigo']][$nome_ekt] = $v['id'];
            }
        }
        
        $nome = $this->vendedores_nomes[$codigo];
        
        if (! $this->vendedores[$codigo][$nome]) {
            die("Vendedor não encontrado para: [" . $codigo . "]" . PHP_EOL . PHP_EOL);
        }
        return $this->vendedores[$codigo][$nome];
    }

    private $gradesTamanhos;

    public function findGradeTamanhoByCodigoAndTamanho($codigo, $tamanho)
    {
        $tamanho = trim($tamanho);
        if (! $this->gradesTamanhos) {
            $this->gradesTamanhos = array();
            $sql = "SELECT gt.id, g.codigo, gt.tamanho FROM est_grade g, est_grade_tamanho gt WHERE gt.grade_id = g.id";
            $r = $this->dbbonerp->query($sql)->result_array();
            foreach ($r as $gt) {
                $_tamanho = trim($gt['tamanho']);
                $this->gradesTamanhos[$gt['codigo']][$_tamanho] = $gt['id'];
            }
        }
        // Se não achar, retorna o 999999 (ERRO DE IMPORTAÇÃO)
        return $this->gradesTamanhos[$codigo][$tamanho] or die("est_grade_tamanho não encontrada para codigo = [" . $codigo . "] e tamanho = [" . $tamanho . "]");
    }
}
