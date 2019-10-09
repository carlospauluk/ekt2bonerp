<?php
require_once ('./application/libraries/file/LogWriter.php');

/**
 * Job que realiza a importação dos dados de vendas da base ekt para a base crosier.
 * crosier
 *
 * Para rodar, pela linha de comando, chamar com:
 *
 *
 * set EKT_CSVS_PATH=\\10.1.1.100\export
 * set EKT_LOG_PATH=C:\ekt2crosier\log
 *
 * export EKT_CSVS_PATH=/mnt/10.1.1.100-export
 * export EKT_LOG_PATH=~/dev/github/ekt2crosier/log
 *
 * php index.php jobs/ekt/ImportarEkt2Espelhos importar YYYYMM FOR-PROD-...
 */
class ImportarVendas extends CI_Controller
{

    private $logger;

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

    private $totalVendasCrosier = 0.0;

    private $totalVendasCrosier_semautorizadas = 0.0;

    /**
     * Conexão ao db ekt.
     */
    private $dbekt;

    /**
     * Conexão ao db crosier.
     */
    private $dbcrosier;

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        
        $this->dbekt = $this->load->database('ekt', TRUE);
        $this->dbcrosier = $this->load->database('crosier', TRUE);
        
        $this->load->library('datetime_library');
        error_reporting(E_ALL);
        ini_set('display_errors', 1);
        
        $this->agora = new DateTime();
        
        $this->load->model('est/produto_model');
        $this->produto_model->setDb($this->dbcrosier);
        
        $this->venda_model = new \CIBases\Models\DAO\Base\Base_model('ven_venda', 'crosier');
        $this->venda_model->setDb($this->dbcrosier);
        
        $this->vendaitem_model = new \CIBases\Models\DAO\Base\Base_model('ven_venda_item', 'crosier');
        $this->vendaitem_model->setDb($this->dbcrosier);
        
        $this->ektvenda_model = new \CIBases\Models\DAO\Base\Base_model('ekt_venda', 'ekt');
        $this->ektvenda_model->setDb($this->dbekt);
        
        $this->pessoa_model = new \CIBases\Models\DAO\Base\Base_model('bse_pessoa', 'crosier');
        $this->pessoa_model->setDb($this->dbcrosier);
        
        $this->funcionario_model = new \CIBases\Models\DAO\Base\Base_model('rh_funcionario', 'crosier');
        $this->funcionario_model->setDb($this->dbcrosier);
    }

    /**
     * Método principal.
     *
     *
     * @param $mesano (yyyymm)
     * @param $acao (VEN,
     *            CORRPLANPAGTO)
     */
    public function importar($acao, $mesano)
    {
        $time_start = microtime(true);
        
        $logPath = getenv('EKT_LOG_PATH') or die("EKT_LOG_PATH não informado" . PHP_EOL . PHP_EOL);
        $prefix = "ImportarVendas" . '_' . $mesano . '_' . "_";
        $this->logger = new LogWriter($logPath, $prefix);
        $this->logger->setLevel(getenv('ekt2crosier_log_level') ? getenv('ekt2crosier_log_level') : 'INFO');
        
        $this->logger->info("Iniciando a importação para o mês/ano: [" . $mesano . "]");
        $this->mesano = $mesano;
        $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01");
        if (! $this->dtMesano instanceof DateTime) {
            $this->logger->info("mesano inválido: [" . $this->mesano . "]");
            return;
        }
        $this->dtMesano->setTime(0, 0, 0, 0);
        
        if (! in_array($acao, array(
            'VEN',
            'CORRPLANPAGTO'
        ))) {
            $this->logger->writeLog("Tipo de importação inválido: [" . $tipo . "]");
        }
        
        if ($acao == 'VEN') {
            $this->marcarDeletadas();
            $this->importarVendas();
        }
        
        if ($acao == 'CORRPLANPAGTO') {
            $this->corrigirPlanosPagto();
        }
        
        $this->logger->info(PHP_EOL . PHP_EOL);
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        $this->logger->info(PHP_EOL . PHP_EOL);
        $this->logger->info("----------------------------------");
        $this->logger->info("Total Execution Time: " . $execution_time . "s");
        
        $this->logger->sendMail();
        $this->logger->closeLog();
    }

    private function importarVendas()
    {
        $this->logger->info("Iniciando a importação de vendas...");
        $this->dbcrosier->trans_start();
        
        $this->importarVendedores();
        
        $l = $this->dbekt->query("SELECT * FROM ekt_venda WHERE mesano = ?", array(
            $this->mesano
        ))->result_array();
        
        // $l = $this->dbekt->query("SELECT * FROM ekt_produto WHERE reduzido = 4521 AND mesano = ?", array($this->mesano))->result_array();
        
        $total = count($l);
        $this->logger->info(" >>>>>>>>>>>>>>>>>>>> " . $total . " venda(s) encontrada(s).");
        
        $i = 0;
        foreach ($l as $ektVenda) {
            $this->logger->debug(" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total);
            $this->importarVenda($ektVenda);
        }
        
        $this->logger->info("Finalizando... commitando a transação...");
        
        $this->dbcrosier->trans_complete();
        
        $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> TOTAL VENDAS EKT: " . $this->totalVendasEkt);
        $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>>>>> TOTAL VENDAS EKT (SEM AUTORIZADAS): " . $this->totalVendasEkt_semautorizadas);
        $valorConf = round($this->totalVendasEkt - $this->totalVendasEkt_semautorizadas, 2);
        $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> VALOR QUE DEVE ESTAR NO RELATÓRIO: " . $valorConf);
        
        $regConf = $this->dbcrosier->query("SELECT valor FROM fin_reg_conf WHERE DATE_FORMAT(dt_registro, '%Y%m') = ? AND descricao = 'TOTAL VENDAS (IMPORTADO)'", array(
            $this->mesano
        ))->result_array();
        
        if (count($regConf) == 1) {
            $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 'TOTAL VENDAS (IMPORTADO)': " . $regConf[0]['valor']);
            $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> DIFERENÇA: " . round($valorConf - $regConf[0]['valor'], 2));
        } else {
            $this->logger->info(" reg conf não encontrado ");
        }
        
        $this->logger->info("");
        $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> TOTAL VENDAS CROSIER: " . $this->totalVendasCrosier);
        $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>> TOTAL VENDAS CROSIER (SEM AUTORIZADAS): " . $this->totalVendasCrosier_semautorizadas);
        
        $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> DIFERENÇA: " . ($this->totalVendasCrosier - $this->totalVendasEkt));
        
        $this->logger->info(PHP_EOL . PHP_EOL);
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("INSERIDAS: " . $this->inseridas);
        $this->logger->info("ATUALIZADAS: " . $this->atualizadas);
        
        $this->logger->info("OK!!!");
    }

    private function importarVendedores()
    {
        $ektVendedores = $this->dbekt->query("SELECT CODIGO, DESCRICAO FROM ekt_vendedor WHERE mesano = ?", array(
            $this->mesano
        ))->result_array();
        
        if (count($ektVendedores) < 1) {
            $this->logger->info("Nenhum ekt_vendedor encontrado no mesano.");
            return;
        }
        
        foreach ($ektVendedores as $ektVendedor) {
            $nomeEkt = trim($ektVendedor['DESCRICAO']);
            if (! $nomeEkt)
                continue;
            $codigo = trim($ektVendedor['CODIGO']);
            if ($codigo == 99)
                continue;
            $vendedor = $this->dbcrosier->query("SELECT 1 FROM rh_funcionario WHERE nome_ekt = ? AND codigo = ?", array(
                $nomeEkt,
                $codigo
            ))->result_array();
            if (! $vendedor) {
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

    private function importarVenda($ektVenda)
    {
        $emissao_mesano = DateTime::createFromFormat('Y-m-d', $ektVenda['EMISSAO'])->format('Ym');
        if ($emissao_mesano != $this->mesano) {
            $this->logger->info("ektvenda.EMISSAO difere do mesano da importação.");
            return;
        }
        
        $params = array(
            $ektVenda['NUMERO'],
            $ektVenda['EMISSAO']
        );
        
        $this->logger->debug(PHP_EOL . ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Importando venda PV = [" . $ektVenda['NUMERO'] . "] em =[" . $ektVenda['EMISSAO'] . "]");
        
        $vendas = $this->dbcrosier->query("SELECT * FROM ven_venda WHERE pv = ? AND dt_venda = ?", $params)->result_array();
        $q = count($vendas);
        
        if ($q > 1) {
            $this->logger->debug("Mais de uma venda encontrada para os dados");
            print_r($params);
            exit();
        }
        
        $venda = null;
        if ($q == 1) {
            $venda = $vendas[0];
            
            $dt_venda_mesano = DateTime::createFromFormat('Y-m-d H:i:s', $venda['dt_venda'])->format('Ym');
            if ($dt_venda_mesano != $this->mesano) {
                $this->logger->info("venda.dt_venda difere do mesano da importação.");
                return;
            }
            if ($venda['mesano'] != $dt_venda_mesano) {
                $this->logger->info("venda.mesano incompatível com venda.dt_venda.");
                return;
            }
            
            $this->deletarItens($venda);
            $this->atualizadas ++;
        } else {
            $this->inseridas ++;
        }
        
        $this->salvarVenda($ektVenda, $venda);
    }

    private function deletarItens($venda)
    {
        if (! $this->dbcrosier->query("DELETE FROM ven_venda_item WHERE venda_id = ?", array(
            $venda['id']
        ))) {
            $this->logger->info("Erro ao deletar itens da venda_id = [" . $venda['id'] . "]");
            return;
        }
    }

    private function salvarVenda($ektVenda, $venda = null)
    {
        if ($venda['deletado'] === true or $ektVenda['COND_PAG'] == '0.99') {
            $venda['deletado'] = true;
        } else {
            $venda['deletado'] = false;
        }
        
        $venda['status'] = 'FINALIZADA';
        $venda['tipo_venda_id'] = 1;
        $venda['mesano'] = $this->mesano;
        $venda['pv'] = $ektVenda['NUMERO'];
        $venda['dt_venda'] = $ektVenda['EMISSAO'];
        
        $venda['plano_pagto_id'] = $this->findPlanoPagto($ektVenda['COND_PAG']);
        
        $venda['vendedor_id'] = $this->findVendedor($ektVenda['VENDEDOR']);
        if (!$venda['vendedor_id']) {
            $this->logger->info('Vendedor não encontrado, portanto pulando venda... ' . $ektVenda['NUMERO'] . ' de ' . $this->mesAno);
            return;
        }
        
        $params = array(
            $this->mesano,
            $ektVenda['NUMERO']
        );
        $ektItens = $this->dbekt->query("SELECT * FROM ekt_venda_item WHERE mesano = ? AND NUMERO_NF = ?", $params)->result_array();
        
        // Para cada item da venda...
        $subTotalVenda = 0.0;
        $venda_itens = array();
        foreach ($ektItens as $ektItem) {
            
            $this->logger->debug("SALVANDO ITEM: " . $ektItem['PRODUTO'] . " - [" . $ektItem['DESCRICAO'] . "]");
            
            $ektProduto = null;
            
            if (! $ektItem['PRODUTO']) {
                $ektItem['PRODUTO'] = "88888";
            }
            
            // Se for um 'NC', não busca.
            if ($ektItem['PRODUTO'] != 88888) {
                $ektProduto = $this->dbekt->query("SELECT * from ekt_produto WHERE REDUZIDO = ? AND mesano = ?", array(
                    $ektItem['PRODUTO'],
                    $this->mesano
                ))->result_array();
                
                if (! $ektProduto or count($ektProduto) == 0) {
                    $this->logger->debug("ekt_produto não encontrado para REDUZIDO = [" . $ektItem['PRODUTO'] . "] e mesano = [" . $this->mesano . "]");
                    $ektItem['PRODUTO'] = 88888;
                } else {
                    if (count($ektProduto) > 1) {
                        $this->logger->info("mais de 1 ekt_produto encontrado para REDUZIDO = [" . $ektItem['PRODUTO'] . "] e mesano = [" . $this->mesano . "]");
                        return;
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
                $this->logger->debug($msg);
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
                    $this->logger->info("est_produto não encontrado para REDUZIDO = [" . $ektItem['PRODUTO'] . "] em mesano = [" . $this->mesano . "]");
                    return;
                }
                $itemVenda['produto_id'] = $produto['id'];
                $itemVenda['grade_tamanho_id'] = $this->findGradeTamanhoByCodigoAndTamanho($ektProduto['GRADE'], $ektItem['TAMANHO']);
                
                $params = array(
                    $produto['id'],
                    $itemVenda['preco_venda'],
                    $itemVenda['preco_venda']
                );
                
                $precos = $this->dbcrosier->query("SELECT * FROM est_produto_preco WHERE produto_id = ? AND (preco_prazo = ? OR preco_promo = ?)", $params)->result_array();
                
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
            $this->logger->debug($msg);
            $venda['obs'] .= PHP_EOL . $msg;
        }
        
        $venda['sub_total'] = $subTotalVenda;
        
        $totalVendaCalculado = $subTotalVenda + $venda['desconto_plano'] + $venda['desconto_especial'];
        
        $totalVendaEKT = (float) $ektVenda['TOTAL'];
        
        $venda['obs'] = "";
        
        if (bccomp($totalVendaCalculado, $totalVendaEKT) != 0) {
            $msg = "********** ATENÇÃO: erro em TOTAL VENDA EKT: [" . $totalVendaEKT . "] TOTAL SOMA: [" . $totalVendaCalculado . "]";
            $this->logger->debug($msg);
            $venda['obs'] .= PHP_EOL . $msg;
        }
        
        // $venda['valor_total'] = $totalVendaCalculado;
        $venda['valor_total'] = $totalVendaEKT;
        
        $venda_id = $this->venda_model->save($venda);
        
        foreach ($venda_itens as $venda_item) {
            $venda_item['venda_id'] = $venda_id;
            $this->vendaitem_model->save($venda_item);
        }
        
        if (! $venda['deletado']) {
            $this->totalVendasEkt += $totalVendaEKT;
            $this->totalVendasEkt_semautorizadas += $ektVenda['COND_PAG'] == '6.00' ? $totalVendaEKT : 0.0;
            $this->totalVendasCrosier += $totalVendaCalculado;
            $this->totalVendasCrosier_semautorizadas += $ektVenda['COND_PAG'] == '6.00' ? $totalVendaCalculado : 0.0;
        }
        $this->logger->debug(">>>>>>>>>>>>>>>> VENDA SALVA");
    }

    /*
     * Corrigindo registros que estavam marcados como 9.99, sendo que deveriam ser 0.99 (cancelados)
     *
     * @param
     * $mesano
     */
    private function corrigirPlanosPagto()
    {
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
                    $this->logger->info("Não encontrado para pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'");
                } else if (count($naVenVenda) == 1) {
                    if ($naVenVenda[0]['plano_pagto_id'] == 2) {
                        $i ++;
                        $this->logger->info("Atualizando o pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'");
                        $this->db->query("UPDATE ven_venda SET plano_pagto_id = 158 WHERE id = ?", $naVenVenda[0]['id']) or exit_db_error();
                    }
                } else {
                    $this->logger->info("Mais de um encontrado para pv = '" . $r['NUMERO'] . "' e mesano = '" . $r['mesano'] . "'");
                    exit();
                }
            } catch (Exception $e) {
                print_r($e->getMessage());
                exit();
            }
        }
        
        $this->logger->info(PHP_EOL . PHP_EOL . "TOTAL ATUALIZADO: " . $i);
        
        $this->db->trans_complete();
    }

    private function exit_db_error()
    {
        $this->logger->info(str_pad("", 100, "*"));
        $this->logger->info("LAST QUERY: " . $this->db->last_query());
        print_r($this->db->error());
        $this->logger->info(str_pad("", 100, "*"));
        exit();
    }

    public function teste($mesano)
    {
        $results = $this->findByReduzidoEkt(1234, '201702');
        print_r($results);
    }

    private $planosPagto;

    private function findPlanoPagto($condPag)
    {
        if (! $this->planosPagto) {
            $r = $this->dbcrosier->query("SELECT id, codigo, descricao FROM ven_plano_pagto")->result_array();
            if (! $r or count($r) < 1) {
                $this->logger->info("Nenhum plano de pagto encontrado na base.");
                exit();
            }
            foreach ($r as $pp) {
                $this->planosPagto[$pp['codigo']] = $pp['id'];
            }
        }
        if (! array_key_exists($condPag, $this->planosPagto)) {
            $this->logger->debug("Plano de pagto não encontrado para: [" . $condPag . "]");
            return $this->planosPagto['9.99'];
        }
        return $this->planosPagto[$condPag];
    }

    private $vendedores;

    private $vendedores_nomes;

    private function findVendedor($codigo)
    {
        if (! $this->vendedores_nomes) {
            $nomes = $this->dbekt->query("SELECT CODIGO, DESCRICAO FROM ekt_vendedor WHERE mesano = ?", array(
                $this->mesano
            ))->result_array();
            if (count($nomes) < 1) {
                $this->logger->info("Erro ao pesquisar nome do vendedor. CODIGO = [" . $codigo . "]");
                return;
            }
            
            foreach ($nomes as $nome) {
                $this->vendedores_nomes[$nome['CODIGO']] = trim($nome['DESCRICAO']);
            }
        }
        
        if (! $this->vendedores) {
            
            $r = $this->dbcrosier->query("SELECT id, codigo, nome_ekt FROM rh_funcionario")->result_array();
            if (count($r) < 1) {
                $this->logger->info("Nenhum vendedor encontrado na base.");
                return;
            }
            foreach ($r as $v) {
                $nome_ekt = trim($v['nome_ekt']);
                $this->vendedores[$v['codigo']][$nome_ekt] = $v['id'];
            }
        }
        
        $nome = $this->vendedores_nomes[$codigo];
        
        if (! $this->vendedores[$codigo][$nome]) {
            $this->logger->info("Vendedor não encontrado para: [" . $codigo . "]");
            return;
        }
        return $this->vendedores[$codigo][$nome];
    }

    private $gradesTamanhos;

    private function findGradeTamanhoByCodigoAndTamanho($codigo, $tamanho)
    {
        $tamanho = trim($tamanho);
        if (! $this->gradesTamanhos) {
            $this->gradesTamanhos = array();
            $sql = "SELECT gt.id, g.codigo, gt.tamanho FROM est_grade g, est_grade_tamanho gt WHERE gt.grade_id = g.id";
            $r = $this->dbcrosier->query($sql)->result_array();
            foreach ($r as $gt) {
                $_tamanho = trim($gt['tamanho']);
                $this->gradesTamanhos[$gt['codigo']][$_tamanho] = $gt['id'];
            }
        }
        
        if (! array_key_exists($codigo, $this->gradesTamanhos) or (! array_key_exists($tamanho, $this->gradesTamanhos[$codigo]))) {
            return 99;
        } else {
            $r = $this->gradesTamanhos[$codigo][$tamanho];
            if (! $r) {
                $this->logger->info("est_grade_tamanho não encontrada para codigo = [" . $codigo . "] e tamanho = [" . $tamanho . "]");
                return;
            } else {
                return $r;
            }
        }
    }

    private function marcarDeletadas()
    {
        $this->logger->info("Marcando vendas deletadas para o mês/ano: [" . $this->mesano . "]");
        
        $vendas = $this->dbcrosier->query("SELECT * FROM ven_venda WHERE deletado IS FALSE AND mesano = ?", array(
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
                $this->logger->info($venda['pv'] . " não existe. Marcando como deletada...");
                $venda['deletado'] = true;
                $this->venda_model->save($venda);
                $deletadas ++;
                $this->logger->info("OK!!!");
            }
        }
        
        $this->logger->info(PHP_EOL . PHP_EOL);
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("DELETADAS: " . $deletadas);
    }
}
