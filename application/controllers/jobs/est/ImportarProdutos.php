<?php
require_once ('./application/libraries/file/LogWriter.php');
require_once ('./application/libraries/util/Datetime_utils.php');

/**
 * Classe responsável por importar produtos das tabelas espelho (ekt_*) para o bonerp.
 *
 * Para rodar:
 *
 * Pela linha de comando, chamar com:
 *
 * (debug? export/set XDEBUG_CONFIG="idekey=session_name")
 *
 * set EKT_CSVS_PATH=\\10.1.1.100\export
 * set EKT_LOG_PATH=C:\ekt2bonerp\log\
 * set XDEBUG_CONFIG="idekey=session_name"
 *
 * LINUX:
 * export EKT_CSVS_PATH=/mnt/10.1.1.100-export/
 * export EKT_LOG_PATH=/home/dev/github/ekt2bonerp/log/
 * export XDEBUG_CONFIG="idekey=session_name"
 *
 * IMPORTAR PRODUTOS:
 * php index.php jobs/est/ImportarProdutos importar PROD YYYYMM
 *
 *
 * CORRIGIR PREÇOS
 * php index.php jobs/est/ImportarProdutos importar PRECOS
 *
 * @author Carlos Eduardo Pauluk
 *        
 */
class ImportarProdutos extends CI_Controller
{

    private $logger;

    private $agora;

    /**
     * Qual a pasta dos CSVs.
     * Será obtido a partir da variável de ambiente EKT_CSVS_PATH.
     *
     * @var string
     */
    private $csvsPath;

    /**
     * Passado pela linha de comando no formato YYYYMM.
     *
     * @var string
     */
    private $mesano;

    /**
     * Se o $mesano = now
     * @var
     */
    private $atual;

    /**
     * Parseado do $mesano para um DateTime.
     *
     * @var DateTime
     */
    private $dtMesano;

    private $produtosaldo_model;

    private $inseridos;

    private $atualizados;

    private $acertados_depara;

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
        
        $this->preco_model = new \CIBases\Models\DAO\Base\Base_model('est_produto_preco', 'bonerp');
        $this->preco_model->setDb($this->dbbonerp);
        
        $this->load->model('est/fornecedor_model');
        $this->fornecedor_model->setDb($this->dbbonerp);
        
        $this->load->model('ekt/ektproduto_model');
        $this->ektproduto_model->setDb($this->dbekt);
        
        $this->produtosaldo_model = new \CIBases\Models\DAO\Base\Base_model('est_produto_saldo', 'bonerp');
        $this->produtosaldo_model->setDb($this->dbbonerp);
    }

    /**
     * Método principal.
     *
     * @param $mesano (yyyymm)
     * @param $acao (PROD,
     *            DEATE)
     */
    public function importar($acao, $mesano = null)
    {
        $time_start = microtime(true);
        
        $this->csvsPath = getenv('EKT_CSVS_PATH') or die("EKT_CSVS_PATH não informado");
        
        $logPath = getenv('EKT_LOG_PATH') or die("EKT_LOG_PATH não informado");
        $prefix = "ImportarProdutos" . '_' . $mesano . '_' . $acao . "_";
        $this->logger = new LogWriter($logPath, $prefix);
        
        $this->logger->info("csvsPath: [" . $this->csvsPath . "]");
        $this->logger->info("logPath: [" . $logPath . "]");
        
        $this->mesano = $mesano;
        $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01");
        if ($this->dtMesano && ! $this->dtMesano instanceof DateTime) {
            $this->logger->info("mesano inválido.");
            $this->logger->sendMail();
            $this->logger->closeLog();
            exit();
        }

        $this->atual = $this->mesano == $this->agora->format('Ym');
        $this->logger->info("Importando 'atual'? " . ($this->atual ? 'SIM' : 'NÃO'));

        if ($acao == 'PROD') {
            $this->logger->info("Iniciando a importação para o mês/ano: [" . $mesano . "]");
            $this->dtMesano->setTime(0, 0, 0, 0);
            $this->logger->info('LIMPANDO A est_produto_saldo...');
            $this->deletarSaldos();
            $this->logger->info("OK!!!");
            
            $this->importarProdutos();
            $this->gerarProdutoSaldoHistorico();
            // $this->corrigirEktDesdeAte();
        }
        if ($acao == 'DEATE') {
            $this->corrigirEktDesdeAte();
        }
        if ($acao == 'PRECOS') {
            $this->corrigirPrecos();
        }
        
        $this->logger->info(PHP_EOL);
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("INSERIDOS: " . $this->inseridos);
        $this->logger->info("ATUALIZADOS: " . $this->atualizados);
        $this->logger->info("ACERTADOS DEPARA: " . $this->acertados_depara);
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        $this->logger->info(PHP_EOL);
        $this->logger->info("----------------------------------");
        $this->logger->info("Total Execution Time: " . $execution_time . "s");
        $this->logger->sendMail();
        $this->logger->closeLog();
    }

    private function importarProdutos()
    {
        $this->logger->info("Iniciando a importação de produtos...");
        $this->dbbonerp->trans_start();
        
        $l = $this->ektproduto_model->findByMesano($this->mesano);
        
        // $l = $this->dbekt->query("SELECT * FROM ekt_produto WHERE reduzido = 4521 AND mesano = ?", array($this->mesano))->result_array();
        
        $total = count($l);
        $this->logger->info(" >>>>>>>>>>>>>>>>>>>> " . $total . " produto(s) encontrado(s).");
        
        $i = 0;
        foreach ($l as $ektProduto) {
            if ($ektProduto['REDUZIDO'] == 88888) {
                continue;
            }
            if (trim($ektProduto['DESCRICAO']) == '') {
                $this->logger->info(" >>>>>>>>>>>>>>>>>>>> PRODUTO com reduzido = '" . $ektProduto['REDUZIDO'] . " está sem descrição. PULANDO.");
                continue;
            }
            $this->logger->debug(" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total);
            $this->importarProduto($ektProduto);
        }
        
        $this->logger->info("Finalizando... commitando a transação...");
        
        $this->dbbonerp->trans_complete();
        
        $this->logger->info("OK!!!");
    }

    private function importarProduto($ektProduto)
    {
        $this->logger->debug(">>>>>>>>>>>>> Trabalhando com " . $ektProduto['REDUZIDO'] . " - [" . $ektProduto['DESCRICAO'] . "]");
        // Verifica produtos com mesmo reduzidoEKT
        $produtosComMesmoReduzidoEKT = $this->produto_model->findByReduzidoEkt($ektProduto['REDUZIDO'], null);
        
        $qtdeComMesmoReduzido = count($produtosComMesmoReduzidoEKT);
        
        // Se não tem nenhum produto com o mesmo reduzido, só insere.
        if ($qtdeComMesmoReduzido == 0) {
            $this->logger->debug("Produto novo. Inserindo...");
            $this->saveProduto($ektProduto, null);
            $this->inseridos ++;
            $this->logger->debug("OK!!!");
        } else {
            $achouMesmo = false;
            $this->logger->debug($qtdeComMesmoReduzido . " produto(s) com o mesmo reduzido. Tentando encontrar o mesmo...");
            // Começa a procurar o mesmo produto
            foreach ($produtosComMesmoReduzidoEKT as $mesmoReduzido) {
                
                $descricao_ekt = trim($ektProduto['DESCRICAO']);
                $descricao = trim($mesmoReduzido['descricao']);
                
                // similar_text($descricao_ekt, $descricao, $percent);
                // $percent = $this->similarity($descricao_ekt,$descricao);
                
                //
                // if ( $percent >= 0.75 or (! $mesmoReduzido['reduzido_ekt_ate']) and $mesmoReduzido['reduzido_ekt_desde'] == $this->dtMesImport) {
                if ($descricao_ekt == $descricao) {
                    // PRODUTO JÁ EXISTENTE
                    $this->logger->debug("Achou o mesmo. Atualizando...");
                    $achouMesmo = true;
                    $this->saveProduto($ektProduto, $mesmoReduzido);
                    $mesmoReduzido = $this->produto_model->findby_id($mesmoReduzido['id']); // recarrego para pegar oq foi alterado
                    
                    $this->saveGrade($ektProduto, $mesmoReduzido);
                    // $mesmoReduzido = $this->produto_model->findby_id($mesmoReduzido['id']);
                    
                    // conferindo se as qtdes na grade batem
                    $qtdeTotal_ektProduto = $this->getQtdeTotalEktProduto($ektProduto);
                    $qtdeTotal_produto = $this->getQtdeTotalProduto($mesmoReduzido);
                    if ($qtdeTotal_ektProduto != $qtdeTotal_produto) {
                        $this->logger->info("Qtde diferem para produtoId=[" . $mesmoReduzido['id'] . "] Reduzido:[" . $ektProduto['REDUZIDO'] . "]");
                        return;
                    }
                    
                    $this->acertaPeriodosReduzidoEKT($mesmoReduzido);
                    
                    $this->atualizados ++;
                    
                    $this->logger->debug("OK!!!");
                    break; // já achou, não precisa continuar procurando
                }
            }
            if (! $achouMesmo) {
                $this->logger->debug("Não achou o mesmo. Salvando um novo produto...");
                $produto = $this->saveProduto($ektProduto, null);
                $this->saveGrade($ektProduto, $produto);
                $this->acertaPeriodosReduzidoEKT($produto);
                $this->logger->debug("OK!!!");
            }
        }
    }

    /**
     *
     * @param
     *            $ektProduto
     * @param
     *            $produto
     */
    private function handleReduzido($ektProduto, $produto = null)
    {
        if ($produto and array_key_exists('reduzido', $produto) and $produto['reduzido']) {
            
            $reduzido_ekt = str_pad($ektProduto['REDUZIDO'], 5, '0', STR_PAD_LEFT);
            $reduzid_bonerp = substr($produto['reduzido'], strlen($produto['reduzido']) - 5);
            
            if ($reduzido_ekt != $reduzid_bonerp) {
                $this->logger->info("Problema com reduzido... bonerp: [" . $produto['reduzido'] . "]. EKT: [" . $ektProduto['REDUZIDO'] . "]");
                return;
            } else {
                return $produto['reduzido'];
            }
        } else {
            // O reduzido do BonERP sempre começa com o mesano
            $mesano_menor = substr($this->mesano, 2);
            $reduzido = $mesano_menor . str_pad($ektProduto['REDUZIDO'], 10, '0', STR_PAD_LEFT);
            
            while (true) {
                $existe = $this->dbbonerp->query("SELECT 1 FROM est_produto WHERE reduzido = ?", array(
                    $reduzido
                ))->result_array();
                if (count($existe) > 0) {
                    $reduzido = $mesano_menor . "0" . str_pad(rand(1, 999), 3, '0', STR_PAD_LEFT) . "0" . str_pad($ektProduto['REDUZIDO'], 5, '0', STR_PAD_LEFT);
                } else {
                    break;
                }
            }
            
            return $reduzido;
        }
    }

    private function saveProduto($ektProduto, $produto = null)
    {
        $produto['depto_imp_id'] = $this->findDeptoBycodigo($ektProduto['DEPTO']);
        if (! $produto['depto_imp_id']) {
            throw new Exception("Depto não encontrado [" . $ektProduto['DEPTO'] . "]");
        }
        
        $produto['subdepto_id'] = $this->findSubdeptoBycodigo($ektProduto['SUBDEPTO']);
        if (! $produto['subdepto_id']) {
            throw new Exception("Subdepto não encontrado [" . $ektProduto['SUBDEPTO'] . "]");
        }
        
        $produto['subdepto_err'] = $this->findSubdeptoBycodigo($ektProduto['SUBDEPTO']);
        
        $fornecedor_id = $this->fornecedor_model->findByCodigoEkt($ektProduto['FORNEC'], $this->mesano);
        if (! $fornecedor_id) {
            throw new Exception("Fornecedor não encontrado: [" . $ektProduto['FORNEC'] . "] no mesano [" . $this->mesano . "]");
        }
        $produto['fornecedor_id'] = $fornecedor_id;
        
        $produto['descricao'] = $ektProduto['DESCRICAO'];
        $produto['dt_ult_venda'] = $ektProduto['DATA_ULT_VENDA'];
        
        $produto['grade_id'] = $this->findGradeByCodigo($ektProduto['GRADE']);
        if (! $produto['grade_id']) {
            throw new Exception("Grade não encontrada [" . $ektProduto['GRADE'] . "]");
        }
        
        $produto['grade_err'] = $ektProduto['GRADE'];
        
        $produto['reduzido'] = $this->handleReduzido($ektProduto, $produto);
        
        $produto['reduzido_ekt'] = $ektProduto['REDUZIDO'];
        
        // if (! $produto['reduzido_ekt_desde']) {
        // $produto['reduzido_ekt_desde'] = $this->dtMesano->format('Y-m') . "01";
        // }
        
        $produto['referencia'] = $ektProduto['REFERENCIA'];
        
        $produto['unidade_produto_id'] = $this->findUnidadeByLabel($ektProduto['UNIDADE']);
        
        if (! $produto['unidade_produto_id']) {
            throw new Exception("Unidade não encontrada: [" . $ektProduto['UNIDADE'] . "]");
        }
        $produto['unidade_produto_err'] = $ektProduto['UNIDADE'];
        
        $produto['cst'] = 102;
        $produto['icms'] = 0;
        $produto['tipo_tributacao'] = "T";
        $produto['ncm'] = $ektProduto['NCM'] ? $ektProduto['NCM'] : "62179000";
        $produto['fracionado'] = $ektProduto['FRACIONADO'] == 'S' ? true : false;

        $produto['atual'] = $this->atual;
        
        $this->logger->debug(" ________________________ save PRODUTO ");
        $produto_id = $this->produto_model->save($produto);
        $produto['id'] = $produto_id;
        $this->logger->debug(" ________________________ OK. id do produto [" . $produto_id . "]");
        
        // Se é uma atualização de produto, verifica se o preço foi alterado
        if ($produto['id']) {
            
            $this->logger->debug("Verificando se já tem o preço cadastrado na est_produto_preco...");
            
            $params = array();
            $params[] = $produto_id;
            $params[] = $ektProduto['DATA_PCUSTO'];
            $params[] = $ektProduto['PCUSTO'];
            $params[] = $ektProduto['PPRAZO'];
            
            $sql = "SELECT 1 FROM est_produto_preco WHERE 
                        produto_id = ? AND 
                        dt_custo = ? AND 
                        preco_custo = ? AND 
                        preco_prazo = ?";
            
            $mesmo = $this->dbbonerp->query($sql, $params)->result_array();
            
            if (! $mesmo) {
                $this->logger->debug("Não tem... salvando o preço...");
                $this->salvarProdutoPreco($ektProduto, $produto_id, $this->mesano);
            }
        } else {
            $this->logger->debug("Inserindo o preço...");
            $this->salvarProdutoPreco($ektProduto, $produto_id, $this->mesano);
        }
        $this->logger->debug("OK!!!");
        
        return $produto;
    }

    /**
     */
    private function saveGrade($ektProduto, $produto)
    {
        $this->logger->debug(">>>>>>>>>>>>>>>>> SALVANDO GRADE PRODUTO...");
        
        $saldos = $this->dbbonerp->query("SELECT count(*) as qtde FROM est_produto_saldo WHERE produto_id = ?", array(
            $produto['id']
        ))->result_array();
        
        if (count($saldos) > 0 && $saldos[0]['qtde'] > 0) {
            throw new Exception("Já tem saldo [" . $produto['descricao'] . "] e não deveria por causa do truncate do começo.");
        }
        
        $qryQtdeTamanhos = $this->dbbonerp->query("SELECT count(*) as qtde FROM est_grade_tamanho WHERE grade_id = ?", array(
            $produto['grade_id']
        ))->result_array();
        if (! $qryQtdeTamanhos[0] or ! $qryQtdeTamanhos[0]['qtde']) {
            throw new Exception("Erro ao pesquisar tamanhos para a grade " . $produto['grade_id']);
        }
        $qtdeTamanhos = $qryQtdeTamanhos[0]['qtde'];
        
        // Em alguns casos tem qtdes em gradestamanho além da capacidade da grade.
        // Aí acumulo tudo e salvo junto numa posição de grade que realmente exista (faça sentido).
        $acumulado = 0.0;
        if ($qtdeTamanhos < 12 and $ektProduto['QT12']) {
            $acumulado += $ektProduto['QT12'];
            $ektProduto['QT12'] = null;
        }
        if ($qtdeTamanhos < 11 and $ektProduto['QT11']) {
            $acumulado += $ektProduto['QT11'];
            $ektProduto['QT11'] = null;
        }
        if ($qtdeTamanhos < 10 and $ektProduto['QT10']) {
            $acumulado += $ektProduto['QT10'];
            $ektProduto['QT10'] = null;
        }
        if ($qtdeTamanhos < 9 and $ektProduto['QT09']) {
            $acumulado += $ektProduto['QT09'];
            $ektProduto['QT09'] = null;
        }
        if ($qtdeTamanhos < 8 and $ektProduto['QT08']) {
            $acumulado += $ektProduto['QT08'];
            $ektProduto['QT08'] = null;
        }
        if ($qtdeTamanhos < 7 and $ektProduto['QT07']) {
            $acumulado += $ektProduto['QT07'];
            $ektProduto['QT07'] = null;
        }
        if ($qtdeTamanhos < 6 and $ektProduto['QT06']) {
            $acumulado += $ektProduto['QT06'];
            $ektProduto['QT06'] = null;
        }
        if ($qtdeTamanhos < 5 and $ektProduto['QT05']) {
            $acumulado += $ektProduto['QT05'];
            $ektProduto['QT05'] = null;
        }
        if ($qtdeTamanhos < 4 and $ektProduto['QT04']) {
            $acumulado += $ektProduto['QT04'];
            $ektProduto['QT04'] = null;
        }
        if ($qtdeTamanhos < 3 and $ektProduto['QT03']) {
            $acumulado += $ektProduto['QT03'];
            $ektProduto['QT03'] = null;
        }
        if ($qtdeTamanhos < 2 and $ektProduto['QT02']) {
            $acumulado += $ektProduto['QT02'];
            $ektProduto['QT02'] = null;
        }
        
        if ($acumulado > 0) {
            $this->logger->debug("Produto com qtdes em posições fora da grade. Acumulado: [" . $acumulado . "]");
        }
        
        for ($i = 1; $i <= 12; $i ++) {
            $this->handleProdutoSaldo($ektProduto, $produto, $i, $acumulado);
            $acumulado = 0.0; // já salvou, não precisa mais
        }
        
        $this->logger->debug(">>>>>>>>>>>>>>>>> OK ");
    }

    private function handleProdutoSaldo($ektProduto, $produto, $ordem, $acumulado)
    {
        $ordemStr = str_pad($ordem, 2, '0', STR_PAD_LEFT);
        
        $qtde = (double) $ektProduto['QT' . $ordemStr];
        
        $qtde += $acumulado;
        
        if ($qtde != 0.0) {
            $this->logger->debug(">>>>>>>>>>>>>>>>> handleProdutoSaldo - " . $ordem);
            
            $qryGt = $this->dbbonerp->query("SELECT gt.id FROM est_grade_tamanho gt, est_grade g WHERE gt.grade_id = g.id AND g.codigo = ? AND gt.ordem = ?", array(
                $ektProduto['GRADE'],
                $ordem
            ))->result_array();
            
            if (count($qryGt) != 1) {
                throw new Exception("Erro ao pesquisar grade. Reduzido: [" . $ektProduto['REDUZIDO'] . "]. Código: [" . $ektProduto['GRADE'] . "]. Ordem: [" . $ordem . "]");
            }
            $gt = $qryGt[0];
            
            $produtoSaldo['produto_id'] = $produto['id'];
            $produtoSaldo['grade_tamanho_id'] = $gt['id'];
            $produtoSaldo['qtde'] = $ektProduto['QT' . $ordemStr] + $acumulado;
            $produtoSaldo['selec'] = $ektProduto['F' . $ordem] == 'S';
            
            $this->produtosaldo_model->save($produtoSaldo) or $this->exit_db_error("Erro ao salvar na est_produto_saldo para o produto id [" . $produto['id'] . "]");
            
            $this->logger->debug(">>>>>>>>>>>>>>>>> OK");
        }
    }

    /**
     *
     * @param
     *            $produtoBonERP
     */
    private function acertaPeriodosReduzidoEKT($produtoBonERP)
    {
        $this->logger->debug(">>>>>>>> ACERTANDO REDUZIDOS EKT: " . $produtoBonERP['reduzido_ekt']);
        
        $corrigiu_algo_aqui = false;
        $produtoId = $produtoBonERP['id'];
        $reduzido_ekt = $produtoBonERP['reduzido_ekt'];
        $this->logger->debug("LIDANDO COM 'depara' [" . $produtoId . "]... ");
        
        // Verifica se já tem registro marcando este produto no mesano
        $sql = "SELECT * FROM est_produto_reduzidoektmesano WHERE produto_id = ? AND mesano = ? AND reduzido_ekt = ?";
        $params = array(
            $produtoId,
            $this->mesano,
            $reduzido_ekt
        );
        $r = $this->dbbonerp->query($sql, $params)->result_array();
        
        // Se ainda não tem na est_produto_reduzidoektmesano, insere...
        if (count($r) == 0) {
            $codektmesano['produto_id'] = $produtoId;
            $codektmesano['mesano'] = $this->mesano;
            $codektmesano['reduzido_ekt'] = $reduzido_ekt;
            
            $this->dbbonerp->insert('est_produto_reduzidoektmesano', $codektmesano) or $this->exit_db_error("Erro ao inserir na est_produto_reduzidoektmesano. produto id [" . $produtoId . "]");
            $corrigiu_algo_aqui = true;
        }
        
        // verifica se o mesano é menor que o reduzido_ekt_desde
        // se for, seta o reduzido_ekt_desde pro mesano
        if (array_key_exists('reduzido_ekt_desde', $produtoBonERP) and $produtoBonERP['reduzido_ekt_desde']) {
            $dt_ekt_desde = DateTime::createFromFormat('Y-m-d', $produtoBonERP['reduzido_ekt_desde']);
            $dt_ekt_desde->setTime(0, 0, 0, 0);
            if ($this->dtMesano < $dt_ekt_desde) {
                $produtoBonERP['reduzido_ekt_desde'] = $this->dtMesano->format('Y-m-d');
                $this->dbbonerp->update('est_produto', $produtoBonERP, array(
                    'id' => $produtoId
                )) or $this->exit_db_error("Erro ao atualizar 'reduzido_ekt_desde'");
                $corrigiu_algo_aqui = true;
            }
        }
        
        // verifica se o mesano é maior que o reduzido_ekt_ate
        // se for, seta o reduzido_ekt_ate pro mesano
        if (array_key_exists('reduzido_ekt_ate', $produtoBonERP) and $produtoBonERP['reduzido_ekt_ate']) {
            $dt_ekt_ate = DateTime::createFromFormat('Y-m-d', $produtoBonERP['reduzido_ekt_ate']);
            $dt_ekt_ate->setTime(0, 0, 0, 0);
            if ($this->dtMesano > $dt_ekt_ate) {
                $produtoBonERP['reduzido_ekt_ate'] = $this->dtMesano->format('Y-m-t');
                $this->dbbonerp->update('est_produto', $produtoBonERP, array(
                    'id' => $produtoId
                )) or $this->exit_db_error("Erro ao atualizar 'reduzido_ekt_ate'");
                $corrigiu_algo_aqui = true;
            }
        }
        
        if ($corrigiu_algo_aqui) {
            $this->acertados_depara ++;
        }
    }

    private function getQtdeTotalEktProduto($ektProduto)
    {
        $qtdeTotal = ($ektProduto['QT01'] ? $ektProduto['QT01'] : 0.0) + ($ektProduto['QT02'] ? $ektProduto['QT02'] : 0.0) + ($ektProduto['QT03'] ? $ektProduto['QT03'] : 0.0) + ($ektProduto['QT04'] ? $ektProduto['QT04'] : 0.0) + ($ektProduto['QT05'] ? $ektProduto['QT05'] : 0.0) + ($ektProduto['QT06'] ? $ektProduto['QT06'] : 0.0) + ($ektProduto['QT07'] ? $ektProduto['QT07'] : 0.0) + ($ektProduto['QT08'] ? $ektProduto['QT08'] : 0.0) + ($ektProduto['QT09'] ? $ektProduto['QT09'] : 0.0) + ($ektProduto['QT10'] ? $ektProduto['QT10'] : 0.0) + ($ektProduto['QT11'] ? $ektProduto['QT11'] : 0.0) + ($ektProduto['QT12'] ? $ektProduto['QT12'] : 0.0);
        
        return $qtdeTotal;
    }

    private function getQtdeTotalProduto($produto)
    {
        if (! $produto) {
            $this->logger->debug("PRODUTO == NULL ?????? ");
            return null;
        }
        $qtdeTotal = 0;
        
        $saldos = $this->dbbonerp->query("SELECT qtde FROM est_produto_saldo WHERE produto_id = ?", array(
            $produto['id']
        ))->result_array();
        
        foreach ($saldos as $saldo) {
            if ($saldo['qtde']) {
                $qtdeTotal += $saldo['qtde'];
            }
        }
        
        return $qtdeTotal;
    }

    private function marcarProdutosInativos()
    {
        // try {
        $this->logger->info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> marcarProdutosInativos");
        
        $sql = "SELECT id, reduzido_ekt FROM est_produto WHERE reduzido_ekt_ate IS NULL";
        $qryProds = $this->dbbonerp->query($sql);
        $prods = $qryProds->result_array();
        
        $this->logger->info("Produtos encontrados: " . count($prods));
        
        $i = 0;
        
        // percorre todos os produtos do BonERP
        foreach ($prods as $p) {
            
            $produtoId = $p['id'];
            $reduzidoEkt = $p['reduzido_ekt'];
            
            $this->logger->debug("PESQUISANDO produtoId: " . $produtoId . " - reduzidoEkt: " . $reduzidoEkt);
            
            $ektProduto = $ektproduto_model->findByReduzido($reduzidoEkt, $mesImport);
            
            if (! $ektProduto == null) {
                $this->logger->debug("Produto: " . $reduzidoEkt . " não encontrado");
                $produto = $this->produto_model->findById($produtoId);
                
                $dtMesImport = clone $this->dtMesImport;
                $dtMesImport->setTime(0, 0, 0, 0);
                $dtMesImport->sub(new \DateInterval('1 month'));
                $ultimoDiaMesAnterior = $dtMesImport->format('Y-m-t');
                
                $produto['reduzido_ekt_ate'] = $ultimoDiaMesAnterior;
                
                $this->produto_model->save($produto);
                $i ++;
            }
        }
        
        $this->logger->info($i + " produto(s) foram 'inativados'");
    }

    private function deletarSaldos()
    {
        $sql = 'TRUNCATE TABLE est_produto_saldo';
        $this->dbbonerp->query($sql) or $this->exit_db_error("Erro ao $sql");
    }

    private function getMesAnoList($dtIni, $dtFim)
    {
        $dtIni = DateTime::createFromFormat('Y-m-d', $dtIni);
        $dtFim = DateTime::createFromFormat('Y-m-d', $dtFim);
        if ($dtFim < $dtIni) {
            return;
        }
        if ($dtIni == null) {
            throw new Exception("dtini null");
        }
        if ($dtFim == null) {
            return;
        }
        $list = array();
        $aux = clone $dtIni;
        $dtFimMesano = $dtFim->format('Ym');
        do {
            $mesano = $aux->format('Ym');
            $list[] = $mesano;
            if ($mesano == $dtFimMesano)
                break;
            $aux->add(new \DateInterval('P1M'));
        } while (true);
        return $list;
    }

    private $deptos;

    private function findDeptoByCodigo($codigo)
    {
        if (! $this->deptos) {
            $this->deptos = array();
            $sql = "SELECT id, codigo FROM est_depto";
            $r = $this->dbbonerp->query($sql)->result_array();
            foreach ($r as $depto) {
                $this->deptos[$depto['codigo']] = $depto['id'];
            }
        }
        return $this->deptos[$codigo] ? $this->deptos[$codigo] : $this->deptos['0'];
    }

    private $subdeptos;

    private function findSubdeptoByCodigo($codigo)
    {
        if (! $this->subdeptos) {
            $this->subdeptos = array();
            $sql = "SELECT id, codigo FROM est_subdepto";
            $r = $this->dbbonerp->query($sql)->result_array();
            foreach ($r as $subdepto) {
                $this->subdeptos[$subdepto['codigo']] = $subdepto['id'];
            }
        }
        if ($this->subdeptos[$codigo]) {
            return $this->subdeptos[$codigo];
        } else {
            return $this->subdeptos['0'];
        }
    }

    private $unidades;

    private function findUnidadeByLabel($label)
    {
        if (! $this->unidades) {
            $this->unidades = array();
            $sql = "SELECT id, label FROM est_unidade_produto";
            $r = $this->dbbonerp->query($sql)->result_array();
            foreach ($r as $unidade) {
                $this->unidades[$unidade['label']] = $unidade['id'];
            }
        }
        if (! $label == null or strpos($label, "PC") >= 0) {
            $label = "UN";
        }
        // Se não achar, retorna o 999999 (ERRO DE IMPORTAÇÃO)
        return $this->unidades[$label] ? $this->unidades[$label] : $this->unidades['ERRO'];
    }

    private $grades;

    private function findGradeByCodigo($codigo)
    {
        if (! $this->grades) {
            $this->grades = array();
            $sql = "SELECT id, codigo FROM est_grade";
            $r = $this->dbbonerp->query($sql)->result_array();
            foreach ($r as $grade) {
                $this->grades[$grade['codigo']] = $grade['id'];
            }
        }
        // Se não achar, retorna o 999999 (ERRO DE IMPORTAÇÃO)
        return $this->grades[$codigo] ? $this->grades[$codigo] : $this->grades['99'];
    }

    private function similarity($s1, $s2)
    {
        $longer = $s1;
        $shorter = $s2;
        if (strlen($s1) < strlen($s2)) { // longer should always have greater length
            $longer = $s2;
            $shorter = $s1;
        }
        $longerLength = strlen($longer);
        if ($longerLength == 0) {
            return 1.0;
        }
        /*
         * // If you have StringUtils, you can use it to calculate the edit distance:
         * return (longerLength - StringUtils.getLevenshteinDistance(longer, shorter)) /
         * (double) longerLength;
         */
        $levenshtein = levenshtein($longer, $shorter);
        $r = ($longerLength - $levenshtein) / $longerLength;
        return $r;
    }

    /**
     *
     * Dei um TRUNCATE nela e fiz este método para ajustar tudo.
     *
     * @param
     *            $mesano
     */
    private function corrigirPrecos()
    {
        $this->dbbonerp->trans_start();
        
        $this->dbbonerp->query("TRUNCATE TABLE est_produto_preco");
        
        $mesano_ini = "201401";
        $hoje = new DateTime();
        $mesano_fim = $hoje->format("Ym");
        
        $mesesanos = Datetime_utils::mesano_list($mesano_ini, $mesano_fim);
        
        foreach ($mesesanos as $mesano) {
            $this->mesano = $mesano;
            
            $query = $this->dbekt->get_where("ekt_produto", array(
                'mesano' => $mesano
            ));
            $result = $query->result_array();
            
            // Pega todos os produtos da ekt_produto para o $mesano
            $i = 1;
            foreach ($result as $r) {
                try {
                    // Para cada ekt_produto, encontra o est_produto
                    $query = $this->dbbonerp->query("SELECT * FROM est_produto WHERE reduzido_ekt = ? AND trim(descricao) LIKE ? ", array(
                        $r['REDUZIDO'],
                        trim($r['DESCRICAO'])
                    ));
                    $result = $query->result_array();
                    $qtde = count($result);
                    if ($qtde != 1) {
                        throw new Exception("Erro. Qtde deveria ser exatamente 1 para reduzido = '" . $r['REDUZIDO'] . "' e descricao = '" . $r['DESCRICAO'] . "'. QTDE='" . $qtde . "'");
                    }
                    $produto = $result[0];
                    $this->salvarProdutoPreco($r, $produto['id'], $mesano);
                    $this->logger->info($i ++ . " (" . $r['id'] . ")");
                } catch (Exception $e) {
                    print_r($e->getMessage());
                    exit();
                }
            }
        }
        
        $this->dbbonerp->trans_complete();
        
        $this->logger->info(PHP_EOL);
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
        $this->logger->info("--------------------------------------------------------------");
    }

    /**
     *
     * @param
     *            $produtoEkt
     * @param
     *            $produtoId
     * @param
     *            $mesano
     */
    private function salvarProdutoPreco($produtoEkt, $produtoId, $mesano)
    {
        if (! $produtoEkt['DATA_PCUSTO']) {
            $produtoEkt['DATA_PCUSTO'] = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
        }
        
        if (! $produtoEkt['DATA_PVENDA']) {
            $produtoEkt['DATA_PVENDA'] = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
        }
        
        $query = $this->dbbonerp->get_where('est_produto_preco', array(
            'produto_id' => $produtoId,
            'dt_custo' => $produtoEkt['DATA_PCUSTO'],
            'preco_custo' => $produtoEkt['PCUSTO'],
            'preco_prazo' => $produtoEkt['PPRAZO']
        ));
        $existe = $query->result_array();
        if (count($existe) > 0) {
            $this->existentes ++;
            return;
        }
        
        $dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
        
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
            'mesano' => $dtMesano
        );
        
        $this->dbbonerp->insert('est_produto_preco', $data) or $this->exit_db_error();
        
        $this->inseridos ++;
    }

    /**
     */
    private function findByReduzidoEkt($reduzidoEkt, $mesano = null)
    {
        $params = array();
        
        $sql = "SELECT id FROM est_produto WHERE reduzido_ekt = ? ";
        $params[] = $reduzidoEkt;
        
        if ($mesano) {
            $sql .= "AND (reduzido_ekt_desde <= ? OR reduzido_ekt_desde IS NULL) " . "AND (reduzido_ekt_ate >= ? OR reduzido_ekt_ate IS NULL) ";
            $params[] = DateTime::createFromFormat('Ym', $mesano)->format('Y-m-d');
            $params[] = DateTime::createFromFormat('Ym', $mesano)->format('Y-m-d');
        }
        
        $sql .= " ORDER BY reduzido_ekt_desde";
        
        $query = $this->dbbonerp->query($sql, $params);
        $result = $query->result_array();
        
        if ($mesano && count($result) > 1) {
            throw new Exception("Mais de um produto com o mesmo reduzido ('$reduzidoEkt) no período ('$mesano')");
        } else {
            return $result;
        }
    }

    private function gerarProdutoSaldoHistorico()
    {
        $this->dbbonerp->trans_start();
        
        $this->logger->info("Iniciando gerarProdutoSaldoHistorico() para mesano = '" . $this->mesano . "'");
        
        $this->dbbonerp->query("DELETE FROM est_produto_saldo_historico WHERE DATE_FORMAT(mesano, '%Y%m') = ?", array(
            $this->mesano
        ));
        
        $ekts = $this->dbekt->query("SELECT
        REDUZIDO,
        coalesce(qt01,0)+coalesce(qt02,0)+coalesce(qt03,0)+coalesce(qt04,0)+coalesce(qt05,0)+coalesce(qt06,0)+
        coalesce(qt07,0)+coalesce(qt08,0)+coalesce(qt09,0)+coalesce(qt10,0)+coalesce(qt11,0)+coalesce(qt12,0) as qtde_total
        FROM
        ekt_produto
        WHERE
        mesano = ? AND REDUZIDO != 88888
        ORDER BY reduzido", array(
            $this->mesano
        ))->result_array();
        
        $total = count($ekts);
        $i = 0;
        
        $r_prods = $this->dbbonerp->query("SELECT reduzido_ekt, produto_id FROM est_produto_reduzidoektmesano WHERE mesano = ?", array(
            $this->mesano
        ))->result_array();
        
        if (count($r_prods) != $total) {
            $this->logger->info("qtde de produtos diferem. EKT: [" . $total . "] est_produto_reduzidoektmesano: [" . count($r_prods) . "]");
            return;
        }
        
        $prods = array();
        foreach ($r_prods as $prod) {
            $prods[$prod['reduzido_ekt']] = $prod['produto_id'];
        }
        
        foreach ($ekts as $ekt) {
            $this->logger->debug(" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total);
            
            $produto_id = $prods[$ekt['REDUZIDO']];
            
            $produtoSaldo['produto_id'] = $produto_id;
            $produtoSaldo['saldo_mes'] = $ekt['qtde_total'];
            $produtoSaldo['mesano'] = $this->dtMesano->format('Y-m-d H:i:s');
            
            $produtoSaldo['updated'] = $this->agora->format('Y-m-d H:i:s');
            $produtoSaldo['inserted'] = $this->agora->format('Y-m-d H:i:s');
            $produtoSaldo['estabelecimento_id'] = 1;
            $produtoSaldo['user_inserted_id'] = 1;
            $produtoSaldo['user_updated_id'] = 1;
            
            $this->dbbonerp->insert('est_produto_saldo_historico', $produtoSaldo) or $this->exit_db_error("Erro ao inserir na est_produto_reduzidoektmesano. produto id [" . $produtoId . "]");
        }
        
        $qry = $this->dbbonerp->query("CALL sp_total_inventario(?,@a,@b,@c)", array(
            $this->mesano
        ));
        $result = $qry->result_array();
        mysqli_next_result($this->dbbonerp->conn_id);
        $qry->free_result();
        
        if (count($result) == 1) {
            $totalCustos = $result[0]['total_custo'];
            $totalPrecosPrazo = $result[0]['total_precos_prazo'];
            $totalPecas = $result[0]['total_pecas'];
            
            $this->logger->info("--------------------------------------------------------------");
            $this->logger->info("Total Custo: " . $totalCustos);
            $this->logger->info("Total Venda: " . $totalPrecosPrazo);
            $this->logger->info("Total Pecas: " . $totalPecas);
            $this->logger->info("--------------------------------------------------------------");
            
            $this->handleRegistroConferencia("INVENT PECAS (IMPORTADO)", $this->dtMesano->format('Y-m-t'), $totalPecas);
            $this->handleRegistroConferencia("INVENT CUSTO (IMPORTADO)", $this->dtMesano->format('Y-m-t'), $totalCustos);
            $this->handleRegistroConferencia("INVENT VENDA (IMPORTADO)", $this->dtMesano->format('Y-m-t'), $totalPrecosPrazo);
        }
        
        $this->logger->info("Finalizando... commitando a transação...");
        
        $this->dbbonerp->trans_complete();
    }

    private function handleRegistroConferencia($descricao, $dtMesano, $valor)
    {
        $this->logger->info("handleRegistroConferencia - [" . $descricao . "]");
        $params = array(
            $descricao,
            $dtMesano
        );
        $r = $this->dbbonerp->query("SELECT id FROM fin_reg_conf WHERE descricao = ? AND dt_registro = ?", $params)->result_array();
        
        $reg['descricao'] = $descricao;
        $reg['dt_registro'] = $dtMesano;
        $reg['valor'] = $valor;
        
        if (count($r) == 1) {
            $reg['id'] = $r[0]['id'];
        }
        $model = new \CIBases\Models\DAO\Base\Base_model('fin_reg_conf', 'bonerp');
        $model->setDb($this->dbbonerp);
        $model->save($reg);
        $this->logger->info("OK!!!");
    }

    private function exit_db_error($msg = null)
    {
        $this->logger->info(str_pad("", 100, "*"));
        $this->logger->info($msg ? $msg : '');
        $this->logger->info("LAST QUERY: " . $this->dbbonerp->last_query());
        print_r($this->dbbonerp->error());
        $this->logger->info(str_pad("", 100, "*"));
        exit();
    }

    /**
     * Corrige os bonerp.est_produto.reduzido para o novo padrão: AAMM0000099999.
     */
    private function corrigirReduzidos()
    {
        $time_start = microtime(true);
        
        $this->logger->info("Iniciando a correção de reduzidos...");
        
        $this->dbbonerp->trans_start();
        
        $r = $this->dbbonerp->query("SELECT id, reduzido, reduzido_ekt FROM est_produto")->result_array();
        $total = count($r);
        $i = 0;
        foreach ($r as $produto) {
            $this->logger->debug(" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total);
            $reduzido = $produto['reduzido'];
            if ($reduzido != 88888) {
                if ($reduzido[0] == 9)
                    continue;
                
                $novo = substr($reduzido, 0, 4) . "00000" . str_pad($produto['reduzido_ekt'], 5, '0', STR_PAD_LEFT);
                if ($novo != $reduzido) {
                    
                    while (true) {
                        $existe = $this->dbbonerp->query("SELECT 1 FROM est_produto WHERE reduzido = ?", array(
                            $novo
                        ))->result_array();
                        if (count($existe) > 0) {
                            $novo = substr($novo, 0, 4) . "0" . str_pad(rand(1, 999), 3, '0', STR_PAD_LEFT) . "0" . str_pad($produto['reduzido_ekt'], 5, '0', STR_PAD_LEFT);
                        } else {
                            break;
                        }
                    }
                    
                    $produto['reduzido'] = $novo;
                    
                    $this->dbbonerp->update('est_produto', $produto, array(
                        'id' => $produto['id']
                    )) or $this->exit_db_error("Erro ao atualizar 'reduzido'");
                }
            }
        }
        $this->logger->info("Finalizando... commitando a transação...");
        
        $this->dbbonerp->trans_complete();
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        $this->logger->info(PHP_EOL);
        $this->logger->info("----------------------------------");
        $this->logger->info("Total Execution Time: " . $execution_time . "s");
        $this->logger->closeLog();
        $this->logger->sendMail();
    }

    /**
     */
    private function corrigirEktDesdeAte()
    {
        $this->dbbonerp->trans_start();
        
        $this->dbbonerp->query("TRUNCATE TABLE est_produto_reduzidoektmesano");
        $this->dbbonerp->query("UPDATE est_produto SET reduzido_ekt_desde = NULL, reduzido_ekt_ate = NULL");
        
        $mesano_ini = "201401";
        $hoje = new DateTime();
        $mesano_fim = $hoje->format("Ym");
        $mesesanos = Datetime_utils::mesano_list($mesano_ini, $mesano_fim);
        
        // Depois que comecei a tratar descrições diferentes como produtos diferentes, esta query deve sempre NÃO retornar nada.
        // select reduzido_ekt, descricao, count(*) as qtde from est_produto group by reduzido_ekt, descricao having qtde > 1;
        
        foreach ($mesesanos as $mesano) {
            
            $this->mesano = $mesano;
            $query = $this->dbekt->get_where("ekt_produto", array(
                'mesano' => $mesano
            ));
            $result = $query->result_array();
            
            // Pega todos os produtos da ekt_produto para o $mesano
            $i = 1;
            foreach ($result as $r) {
                try {
                    // Para cada ekt_produto, encontra o est_produto
                    $query = $this->dbbonerp->query("SELECT * FROM est_produto WHERE reduzido_ekt = ? AND trim(descricao) LIKE ? ", array(
                        $r['REDUZIDO'],
                        trim($r['DESCRICAO'])
                    ));
                    $result = $query->result_array();
                    $qtde = count($result);
                    if ($qtde != 1) {
                        throw new Exception("Erro. Qtde deveria ser exatamente 1 para reduzido = '" . $r['REDUZIDO'] . "' e descricao = '" . $r['DESCRICAO'] . "'. QTDE='" . $qtde . "'");
                    }
                    $this->acertaPeriodosReduzidoEKT($result[0]);
                    $this->logger->info($i ++ . " (" . $r['id'] . ")");
                } catch (Exception $e) {
                    print_r($e->getMessage());
                    exit();
                }
            }
        }
        
        // Pego TODOS os produtos da est_produto
        $prods = $this->dbbonerp->query("SELECT id, reduzido_ekt_desde, reduzido_ekt_ate FROM est_produto WHERE reduzido_ekt is not null AND reduzido_ekt != '88888'")->result_array();
        // $prods = $this->dbbonerp->query("SELECT id, reduzido_ekt_desde, reduzido_ekt_ate FROM est_produto WHERE reduzido_ekt = '2173'")->result_array();
        
        $i = 0;
        $total = count($prods);
        
        foreach ($prods as $prod) {
            
            // Verifico a lista de mesesanos que foi montada logo acima
            $ekts = $this->dbbonerp->query("SELECT mesano FROM est_produto_reduzidoektmesano WHERE produto_id = ?", array(
                $prod['id']
            ))->result_array();
            
            if (! $ekts or count($ekts) < 1) {
                $this->logger->info("est_produto_reduzidoektmesano não encontrado para est_produto.id = [" . $prod['id'] . "]");
                return;
            }
            
            $desde = DateTime::createFromFormat('Ym', $ekts[0]['mesano'])->format('Y-m-') . "01";
            $ate = DateTime::createFromFormat('Ym', $ekts[count($ekts) - 1]['mesano'])->format('Y-m-t');
            
            $prod['reduzido_ekt_desde'] = $desde;
            $prod['reduzido_ekt_ate'] = $ate;
            
            $this->dbbonerp->update('est_produto', $prod, array(
                'id' => $prod['id']
            )) or $this->exit_db_error("Erro ao atualizar 'reduzido_ekt_desde'");
            
            $this->logger->debug(" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total);
        }
        
        $this->logger->info("Finalizando... commitando a transação...");
        
        $this->dbbonerp->trans_complete();
    }
}





