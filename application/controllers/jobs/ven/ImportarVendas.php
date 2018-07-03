<?php

/**
 * Job que realiza a importação dos dados de vendas da base ekt para a base bonerp. bonerp
 *
 * Para rodar, pela linha de comando, chamar com:
 *
 *
 * set EKT_CSVS_PATH=\\10.1.1.100\export
 * set EKT_LOG_PATH=C:\ekt2bonerp\log
 *
 * export EKT_CSVS_PATH=/mnt/10.1.1.100-export
 * export EKT_LOG_PATH=~/dev/github/ekt2bonerp/log
 *
 * php index.php jobs/ekt/ImportarEkt2Espelhos importar YYYYMM FOR-PROD-...
 */
class ImportarVendas extends CI_Controller
{

    private $agora;

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

    private $totalVendasEkt = 0.0;

    private $totalVendasEkt_semautorizadas = 0.0;

    private $totalVendasBonerp = 0.0;

    private $totalVendasBonerp_semautorizadas = 0.0;

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
        
        $this->pessoa_model = new \CIBases\Models\DAO\Base\Base_model('bon_pessoa', 'bonerp');
        $this->pessoa_model->setDb($this->dbbonerp);
        
        $this->funcionario_model = new \CIBases\Models\DAO\Base\Base_model('rh_funcionario', 'bonerp');
        $this->funcionario_model->setDb($this->dbbonerp);
    }

    /**
     * Método principal.
     *
     * @param $mesano (yyyymm)
     * @param $acao (PROD,
     *            DEATE)
     */
    public function importar($mesano)
    {
        $time_start = microtime(true);
        
        echo PHP_EOL . PHP_EOL;
        
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
        
        echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> TOTAL VENDAS EKT: " . $this->totalVendasEkt . PHP_EOL;
        echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>> TOTAL VENDAS EKT (SEM AUTORIZADAS): " . $this->totalVendasEkt_semautorizadas . PHP_EOL;
        $valorConf = round($this->totalVendasEkt - $this->totalVendasEkt_semautorizadas, 2);
        echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>> VALOR QUE DEVE ESTAR NO RELATÓRIO: " . $valorConf . PHP_EOL;
        
        $regConf = $this->dbbonerp->query("SELECT valor FROM fin_reg_conf WHERE DATE_FORMAT(dt_registro, '%Y%m') = ? AND descricao = 'TOTAL VENDAS (IMPORTADO)'", array(
            $this->mesano
        ))->result_array();
        
        if (count($regConf) == 1) {
            echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 'TOTAL VENDAS (IMPORTADO)': " . $regConf[0]['valor'] . PHP_EOL;
            echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> DIFERENÇA: " . round($valorConf - $regConf[0]['valor'], 2) . PHP_EOL . PHP_EOL;
        } else {
            echo " reg conf não encontrado " . PHP_EOL;
        }
        
        echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> TOTAL VENDAS BONERP: " . $this->totalVendasBonerp . PHP_EOL;
        echo ">>>>>>>>>>>>>>>>>>>>>>>>> TOTAL VENDAS BONERP (SEM AUTORIZADAS): " . $this->totalVendasBonerp_semautorizadas . PHP_EOL;
        
        echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> DIFERENÇA: " . ($this->totalVendasBonerp - $this->totalVendasEkt) . PHP_EOL;
        
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
        
        
        $this->importarVendedores();
        
        
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
    
    public function importarVendedores() {
        $ektVendedores = $this->dbekt->query("SELECT CODIGO, DESCRICAO FROM ekt_vendedor WHERE mesano = ?", array($this->mesano))->result_array();
        
        if (count($ektVendedores) < 1) {
            die("Nenhum ekt_vendedor encontrado no mesano." . PHP_EOL);
        }
        
        foreach ($ektVendedores as $ektVendedor) {
            $nomeEkt = trim($ektVendedor['DESCRICAO']);
            if (!$nomeEkt) continue;
            $codigo = trim($ektVendedor['CODIGO']);
            if ($codigo == 99) continue;
            $vendedor = $this->dbbonerp->query("SELECT 1 FROM rh_funcionario WHERE nome_ekt = ? AND codigo = ?", array($nomeEkt, $codigo))->result_array();
            if (!$vendedor) {
                $pessoa['nome'] = $nomeEkt;
                $pessoaId = $this->pessoa_model->save($pessoa);
                
                $funcionario['clt'] = true;
                $funcionario['nome_ekt'] = $nomeEkt;
                $funcionario['codigo'] = $codigo;
                $funcionario['vendedor_comissionado'] = true;
                $funcionario['pessoa_id'] = $pessoaId;
                $this->funcionario_model->save($funcionario);                
            }
        }
        
    }
    

    public function importarVenda($ektVenda)
    {
        $emissao_mesano = DateTime::createFromFormat('Y-m-d', $ektVenda['EMISSAO'])->format('Ym');
        if ($emissao_mesano != $this->mesano) {
            die("ektvenda.EMISSAO difere do mesano da importação.");
        }
        
        $params = array(
            $ektVenda['NUMERO'],
            $ektVenda['EMISSAO']
        );
        
        echo PHP_EOL . ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Importando venda PV = [" . $ektVenda['NUMERO'] . "] em =[" . $ektVenda['EMISSAO'] . "]" . PHP_EOL;
        
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
            
            $dt_venda_mesano = DateTime::createFromFormat('Y-m-d H:i:s', $venda['dt_venda'])->format('Ym');
            if ($dt_venda_mesano != $this->mesano) {
                die("venda.dt_venda difere do mesano da importação.");
            }
            if ($venda['mesano'] != $dt_venda_mesano) {
                die("venda.mesano incompatível com venda.dt_venda.");
            }
            
            $this->deletarItens($venda);
            $this->atualizadas ++;
        } else {
            $this->inseridas ++;
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
        $venda['deletado'] = false;
        
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
            
            $ektProduto = null;
            
            if (!$ektItem['PRODUTO']) {
                $ektItem['PRODUTO'] = "88888";
            }
            
            // Se for um 'NC', não busca.
            if ($ektItem['PRODUTO'] != 88888) {
                $ektProduto = $this->dbekt->query("SELECT * from ekt_produto WHERE REDUZIDO = ? AND mesano = ?", array(
                    $ektItem['PRODUTO'],
                    $this->mesano
                ))->result_array();
                
                if (! $ektProduto or count($ektProduto) == 0) {
                    echo "ekt_produto não encontrado para REDUZIDO = [" . $ektItem['PRODUTO'] . "] e mesano = [" . $this->mesano . "]" . PHP_EOL;
                    $ektItem['PRODUTO'] = 88888;
                } else {
                    if (count($ektProduto) > 1) {
                        die("mais de 1 ekt_produto encontrado para REDUZIDO = [" . $ektItem['PRODUTO'] . "] e mesano = [" . $this->mesano . "]");
                    } else {
                        $ektProduto = $ektProduto[0];
                    }
                }
            }
            
            $itemVenda = array();
            
            $itemVenda['obs'] = "";
            
            $itemVenda['qtde'] = $ektItem['QTDE'];
            $itemVenda['preco_venda'] = $ektItem['VLR_UNIT'];
            
            $valorTotal = round($itemVenda['qtde'] * $itemVenda['preco_venda'], 2);
            
            if (bccomp($valorTotal, $ektItem['VLR_TOTAL']) != 0) {
                $msg = "********** ATENÇÃO: erro em total de produto importado. Total Produto EKT: " . $valorTotal . ". Total Calculado: " . $ektItem['VLR_TOTAL'];
                echo $msg . PHP_EOL;
                $itemVenda['obs'] .= PHP_EOL . $msg;
            }
            
            $itemVenda['nc_descricao'] = $ektItem['DESCRICAO'];
            $itemVenda['nc_reduzido'] = $ektItem['PRODUTO'];
            $itemVenda['nc_grade_tamanho'] = $ektItem['TAMANHO'];
            
            // Para NCs
            if ($ektItem['PRODUTO'] == 88888) {
                $itemVenda['obs'] .= PHP_EOL . "NC 88888";
                $itemVenda['grade_tamanho_id'] = 2;
            } else {
                
                $produto = $this->produto_model->findByReduzidoEktAndMesano($ektItem['PRODUTO'], $this->mesano);
                if (! $produto) {
                    die("est_produto não encontrado para REDUZIDO = [" . $ektItem['PRODUTO'] . "] em mesano = [" . $this->mesano . "]" . PHP_EOL . PHP_EOL);
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
        
        if (bccomp($subTotalVenda, $ektVenda['SUB_TOTAL']) != 0) {
            $msg = "********** ATENÇÃO: erro em SUB TOTAL VENDA: " . $ektVenda['SUB_TOTAL'] . ". TOTAL SOMADO: " . $subTotalVenda;
            echo $msg . PHP_EOL;
            $venda['obs'] .= PHP_EOL . $msg;
        }
        
        $venda['sub_total'] = $subTotalVenda;
        
        $totalVendaCalculado = $subTotalVenda + $venda['desconto_plano'] + $venda['desconto_especial'];
        
        $totalVendaEKT = (float) $ektVenda['TOTAL'];
        
        if (bccomp($totalVendaCalculado, $totalVendaEKT) != 0) {
            $msg = "********** ATENÇÃO: erro em TOTAL VENDA EKT: [" . $totalVendaEKT . "] TOTAL SOMA: [" . $totalVendaCalculado . "]";
            echo $msg . PHP_EOL;
            $venda['obs'] .= PHP_EOL . $msg;
        }
        
        // $venda['valor_total'] = $totalVendaCalculado;
        $venda['valor_total'] = $totalVendaEKT;
        
        $venda_id = $this->venda_model->save($venda);
        
        foreach ($venda_itens as $venda_item) {
            $venda_item['venda_id'] = $venda_id;
            $this->vendaitem_model->save($venda_item);
        }
        
        $this->totalVendasEkt += $totalVendaEKT;
        $this->totalVendasEkt_semautorizadas += $ektVenda['COND_PAG'] == '6.00' ? $totalVendaEKT : 0.0;
        $this->totalVendasBonerp += $totalVendaCalculado;
        $this->totalVendasBonerp_semautorizadas += $ektVenda['COND_PAG'] == '6.00' ? $totalVendaCalculado : 0.0;
        
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
            $r = $this->dbbonerp->query("SELECT id, codigo, descricao FROM ven_plano_pagto")->result_array();
            if (! $r or count($r) < 1) {
                die("Nenhum plano de pagto encontrado na base.");
            }
            foreach ($r as $pp) {
                $this->planosPagto[$pp['codigo']] = $pp['id'];
            }
        }
        if (! array_key_exists($condPag, $this->planosPagto)) {
            echo "Plano de pagto não encontrado para: [" . $condPag . "]" . PHP_EOL . PHP_EOL;
            return $this->planosPagto['9.99'];
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
        
        if (! array_key_exists($codigo, $this->gradesTamanhos) or (! array_key_exists($tamanho, $this->gradesTamanhos[$codigo]))) {
            return 99;
        } else {
            return $this->gradesTamanhos[$codigo][$tamanho] or die("est_grade_tamanho não encontrada para codigo = [" . $codigo . "] e tamanho = [" . $tamanho . "]");
        }
    }

    public function marcarDeletadas($mesano)
    {
        $time_start = microtime(true);
        
        echo PHP_EOL . PHP_EOL;
        
        echo "Marcando vendas deletadas para o mês/ano: [" . $mesano . "]" . PHP_EOL;
        $this->mesano = $mesano;
        $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01");
        if (! $this->dtMesano instanceof DateTime) {
            die("mesano inválido." . PHP_EOL . PHP_EOL . PHP_EOL);
        }
        $this->dtMesano->setTime(0, 0, 0, 0);
        
        $vendas = $this->dbbonerp->query("SELECT * FROM ven_venda WHERE deletado IS FALSE AND mesano = ?", array(
            $this->mesano
        ))->result_array();
        
        $ektVendas = $this->dbekt->query("SELECT NUMERO FROM ekt_venda WHERE mesano = ?", array(
            $this->mesano
        ))->result_array();
        
        $ektVendaz = array();
        
        foreach ($ektVendas as $ektVenda) {
            $ektVendaz[] = $ektVenda['NUMERO'];
        }
        
        $deletadas = 0;
        foreach ($vendas as $venda) {
            if (! array_search($venda['pv'], $ektVendaz)) {
                echo $venda['pv'] . " não existe. Marcando como deletada..." . PHP_EOL;
                $venda['deletado'] = true;
                $this->venda_model->save($venda);
                $deletadas ++;
                echo "OK!!!" . PHP_EOL . PHP_EOL;
            }
        }
        
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "DELETADAS: " . $deletadas . PHP_EOL;
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "----------------------------------" . PHP_EOL;
        echo "Total Execution Time: " . $execution_time . "s" . PHP_EOL . PHP_EOL . PHP_EOL;
    }
}
