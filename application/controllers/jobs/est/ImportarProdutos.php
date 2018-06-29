<?php

/**
 * Classe responsável por importar produtos das tabelas espelho (ekt_*) para o bonerp.
 * 
 * Para rodar:
 * 
 * Pela linha de comando, chamar com:
 *
 *
 * set EKT_CSVS_PATH=\\10.1.1.100\export
 * set EKT_LOG_PATH=C:\ekt2bonerp\log\
 *
 * export EKT_CSVS_PATH=/mnt/10.1.1.100-export/
 * export EKT_LOG_PATH=~/dev/github/ekt2bonerp/log/
 *
 * php index.php jobs/ekt/ImportarProdutos importar PROD/DEATE YYYYMM ... 
 * 
 * @author Carlos Eduardo Pauluk
 *
 */
class ImportarProdutos extends CI_Controller
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
        
        echo PHP_EOL . PHP_EOL;
        
        $this->csvsPath = getenv('EKT_CSVS_PATH') or die("EKT_CSVS_PATH não informado" . PHP_EOL . PHP_EOL . PHP_EOL);
        $this->logPath = getenv('EKT_LOG_PATH') or die("EKT_LOG_PATH não informado" . PHP_EOL . PHP_EOL . PHP_EOL);
        
        echo "csvsPath: [" . $this->csvsPath . "]" . PHP_EOL;
        echo "logPath: [" . $this->logPath . "]" . PHP_EOL;
        $this->logFile = $this->logPath . "espelhos2bonerp-PROD-" . $this->agora->format('Y-m-d_H-i-s') . ".txt";
        echo "logFile: [" . $this->logFile . "]" . PHP_EOL;
        
        if ($acao == 'PROD') {
            echo "Iniciando a importação para o mês/ano: [" . $mesano . "]" . PHP_EOL;
            $this->mesano = $mesano;
            $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01");
            if (! $this->dtMesano instanceof DateTime) {
                die("mesano inválido." . PHP_EOL . PHP_EOL . PHP_EOL);
            }
            echo 'LIMPANDO A est_produto_saldo...' . PHP_EOL;
            $this->deletarSaldos();
            echo "OK!!!" . PHP_EOL . PHP_EOL;
            
            $this->importarProdutos();
        }
        
        if ($acao == 'DEATE') {
            $this->acertarDeAteProdutos();
        }
        
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "INSERIDOS: " . $this->inseridos . PHP_EOL;
        echo "ATUALIZADOS: " . $this->atualizados . PHP_EOL;
        echo "ACERTADOS DEPARA: " . $this->acertados_depara . PHP_EOL;
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "----------------------------------" . PHP_EOL;
        echo "Total Execution Time: " . $execution_time . "s" . PHP_EOL . PHP_EOL . PHP_EOL;
    }

    public function importarProdutos()
    {
        echo "Iniciando a importação de produtos..." . PHP_EOL;
        $this->dbbonerp->trans_start();
        
        $l = $this->ektproduto_model->findByMesano($this->mesano);
        
        // $l = $this->dbekt->query("SELECT * FROM ekt_produto WHERE reduzido = 4521 AND mesano = ?", array($this->mesano))->result_array();
        
        $total = count($l);
        echo " >>>>>>>>>>>>>>>>>>>> " . $total . " produto(s) encontrado(s)." . PHP_EOL;
        
        $i = 0;
        foreach ($l as $ektProduto) {
            if ($ektProduto['REDUZIDO'] == 88888) {
                continue;
            }
            echo " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total . PHP_EOL;
            $this->importarProduto($ektProduto);
        }
        
        echo "Finalizando... commitando a transação..." . PHP_EOL;
        
        $this->dbbonerp->trans_complete();
        
        echo "OK!!!" . PHP_EOL;
    }

    public function importarProduto($ektProduto)
    {
        echo ">>>>>>>>>>>>> Trabalhando com " . $ektProduto['REDUZIDO'] . " - [" . $ektProduto['DESCRICAO'] . "]" . PHP_EOL;
        // Verifica produtos com mesmo reduzidoEKT
        $produtosComMesmoReduzidoEKT = $this->produto_model->findByReduzidoEkt($ektProduto['REDUZIDO'], null);
        
        $qtdeComMesmoReduzido = count($produtosComMesmoReduzidoEKT);
        
        // Se não tem nenhum produto com o mesmo reduzido, só insere.
        if ($qtdeComMesmoReduzido == 0) {
            echo "Produto novo. Inserindo..." . PHP_EOL;
            $this->saveProduto($ektProduto, null);
            $this->inseridos ++;
            echo "OK!!!" . PHP_EOL;
        } else {
            $achouMesmo = false;
            echo $qtdeComMesmoReduzido . " produto(s) com o mesmo reduzido. Tentando encontrar o mesmo..." . PHP_EOL;
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
                    echo "Achou o mesmo. Atualizando..." . PHP_EOL;
                    $achouMesmo = true;
                    $this->saveProduto($ektProduto, $mesmoReduzido);
                    $mesmoReduzido = $this->produto_model->findby_id($mesmoReduzido['id']); // recarrego para pegar oq foi alterado
                    
                    $this->saveGrade($ektProduto, $mesmoReduzido);
                    // $mesmoReduzido = $this->produto_model->findby_id($mesmoReduzido['id']);
                    
                    // conferindo se as qtdes na grade batem
                    $qtdeTotal_ektProduto = $this->getQtdeTotalEktProduto($ektProduto);
                    $qtdeTotal_produto = $this->getQtdeTotalProduto($mesmoReduzido);
                    if ($qtdeTotal_ektProduto != $qtdeTotal_produto) {
                        die("Qtde diferem para produtoId=[" . $mesmoReduzido['id'] . "] Reduzido:[" . $ektProduto['REDUZIDO'] . "]" . PHP_EOL . PHP_EOL . PHP_EOL);
                    }
                    
                    $this->acertaPeriodosReduzidoEKT($mesmoReduzido);
                    
                    $this->atualizados ++;
                    
                    echo "OK!!!" . PHP_EOL;
                    break; // já achou, não precisa continuar procurando
                }
            }
            if (! $achouMesmo) {
                echo "Não achou o mesmo. Salvando um novo produto..." . PHP_EOL;
                $produto = $this->saveProduto($ektProduto, null);
                $this->saveGrade($ektProduto, $produto);
                $this->acertaPeriodosReduzidoEKT($produto);
                echo "OK!!!" . PHP_EOL;
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
    public function handleReduzido($ektProduto, $produto = null)
    {
        if ($produto and array_key_exists('reduzido', $produto) and $produto['reduzido']) {
            
            $reduzido_ekt = str_pad($ektProduto['REDUZIDO'], 5, '0', STR_PAD_LEFT);
            $reduzid_bonerp = substr($produto['reduzido'], strlen($produto['reduzido']) - 5);
            
            if ($reduzido_ekt != $reduzid_bonerp) {
                die("Problema com reduzido... bonerp: [" . $produto['reduzido'] . "]. EKT: [" . $ektProduto['REDUZIDO'] . "]" . PHP_EOL . PHP_EOL . PHP_EOL);
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

    public function saveProduto($ektProduto, $produto = null)
    {
        $produto['depto_imp_id'] = $this->findDeptoBycodigo($ektProduto['DEPTO']) or die("Depto não encontrado [" . $ektProduto['DEPTO'] . "]" . PHP_EOL . PHP_EOL . PHP_EOL);
        $produto['subdepto_id'] = $this->findSubdeptoBycodigo($ektProduto['SUBDEPTO']) or die("Subdepto não encontrado [" . $ektProduto['SUBDEPTO'] . "]" . PHP_EOL . PHP_EOL . PHP_EOL);
        $produto['subdepto_err'] = $this->findSubdeptoBycodigo($ektProduto['SUBDEPTO']);
        $fornecedor_id = $this->fornecedor_model->findByCodigoEkt($ektProduto['FORNEC'], $this->mesano) or die("Fornecedor não encontrado: [" . $ektProduto['FORNEC'] . "] no mesano [" . $this->mesano . "]" . PHP_EOL . PHP_EOL . PHP_EOL);
        $produto['fornecedor_id'] = $fornecedor_id;
        
        $produto['descricao'] = $ektProduto['DESCRICAO'];
        $produto['dt_ult_venda'] = $ektProduto['DATA_ULT_VENDA'];
        $produto['grade_id'] = $this->findGradeByCodigo($ektProduto['GRADE']) or die("Grade não encontrada [" . $ektProduto['GRADE'] . "]" . PHP_EOL . PHP_EOL . PHP_EOL);
        $produto['grade_err'] = $ektProduto['GRADE'];
        
        $produto['reduzido'] = $this->handleReduzido($ektProduto, $produto);
        
        $produto['reduzido_ekt'] = $ektProduto['REDUZIDO'];
        
        // if (! $produto['reduzido_ekt_desde']) {
        // $produto['reduzido_ekt_desde'] = $this->dtMesano->format('Y-m') . "01";
        // }
        
        $produto['referencia'] = $ektProduto['REFERENCIA'];
        
        $produto['unidade_produto_id'] = $this->findUnidadeByLabel($ektProduto['UNIDADE']) or die("Unidade não encontrada: [" . $ektProduto['UNIDADE'] . "]" . PHP_EOL . PHP_EOL . PHP_EOL);
        $produto['unidade_produto_err'] = $ektProduto['UNIDADE'];
        
        $produto['cst'] = 102;
        $produto['icms'] = 0;
        $produto['tipo_tributacao'] = "T";
        $produto['ncm'] = $ektProduto['NCM'] ? $ektProduto['NCM'] : "62179000";
        $produto['fracionado'] = $ektProduto['FRACIONADO'] == 'S' ? true : false;
        
        echo " ________________________ save PRODUTO " . PHP_EOL;
        $produto_id = $this->produto_model->save($produto);
        $produto['id'] = $produto_id;
        echo " ________________________ OK. id do produto [" . $produto_id . "]" . PHP_EOL;
        
        // Se é uma atualização de produto, verifica se o preço foi alterado
        if ($produto['id']) {
            
            echo "Verificando se já tem o preço cadastrado na est_produto_preco..." . PHP_EOL;
            
            $params = array();
            $params[] = $produto_id;
            $params[] = $ektProduto['DATA_PCUSTO'];
            $params[] = $ektProduto['PCUSTO'];
            $params[] = $ektProduto['PPRAZO'];
            
            //@formatter:off
            $sql = "SELECT 1 FROM est_produto_preco WHERE " 
                    . "produto_id = ? AND " . 
                    "dt_custo = ? AND " . 
                    "preco_custo = ? AND " . 
                    "preco_prazo = ? AND " . 
                    "1=1";
            // @formatter:on
            
            $mesmo = $this->dbbonerp->query($sql, $params)->result_array();
            
            if (! $mesmo) {
                echo "Não tem... salvando o preço..." . PHP_EOL;
                $this->savePreco($ektProduto, $produto_id);
            }
        } else {
            echo "Inserindo o preço..." . PHP_EOL;
            $this->savePreco($ektProduto, $produto_id);
        }
        echo "OK!!!" . PHP_EOL;
        
        return $produto;
    }

    public function savePreco($ektProduto, $produto_id)
    {
        $preco['mesano'] = $this->mesano;
        $preco['produto_id'] = $produto_id;
        $preco['coeficiente'] = $ektProduto['COEF'];
        $preco['custo_operacional'] = $ektProduto['MARGEMC'];
        $preco['custo_financeiro'] = 0.15;
        $preco['margem'] = $ektProduto['MARGEM'];
        $preco['dt_custo'] = $ektProduto['DATA_PCUSTO'];
        $preco['dt_preco_venda'] = $ektProduto['DATA_PVENDA'];
        $preco['prazo'] = $ektProduto['PRAZO'];
        $preco['preco_custo'] = $ektProduto['PCUSTO'];
        $preco['preco_prazo'] = $ektProduto['PPRAZO'];
        $preco['preco_promo'] = $ektProduto['PPROMO'];
        $preco['preco_vista'] = $ektProduto['PVISTA'];
        
        $this->preco_model->save($preco) or $this->exit_db_error("Erro ao salvar o preço para o produto id [" . $produto_id . "]");
    }

    /**
     */
    public function saveGrade($ektProduto, $produto)
    {
        echo ">>>>>>>>>>>>>>>>> SALVANDO GRADE PRODUTO..." . PHP_EOL;
        
        $saldos = $this->dbbonerp->query("SELECT count(*) as qtde FROM est_produto_saldo WHERE produto_id = ?", array(
            $produto['id']
        ))->result_array();
        
        if (count($saldos) > 0 && $saldos[0]['qtde'] > 0) {
            die("Já tem saldo [" . $produto['descricao'] . "] e não deveria por causa do truncate do começo." . PHP_EOL . PHP_EOL . PHP_EOL);
        }
        
        $qryQtdeTamanhos = $this->dbbonerp->query("SELECT count(*) as qtde FROM est_grade_tamanho WHERE grade_id = ?", array(
            $produto['grade_id']
        ))->result_array();
        if (! $qryQtdeTamanhos[0] or ! $qryQtdeTamanhos[0]['qtde']) {
            die("Erro ao pesquisar tamanhos para a grade " . $produto['grade_id'] . PHP_EOL . PHP_EOL . PHP_EOL);
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
            echo "Produto com qtdes em posições fora da grade. Acumulado: [" . $acumulado . "]" . PHP_EOL;
        }
        
        for ($i = 1; $i <= 12; $i ++) {
            $this->handleProdutoSaldo($ektProduto, $produto, $i, $acumulado);
            $acumulado = 0.0; // já salvou, não precisa mais
        }
        
        echo ">>>>>>>>>>>>>>>>> OK " . PHP_EOL;
    }

    public function handleProdutoSaldo($ektProduto, $produto, $ordem, $acumulado)
    {
        $ordemStr = str_pad($ordem, 2, '0', STR_PAD_LEFT);
        
        $qtde = (double) $ektProduto['QT' . $ordemStr];
        
        $qtde += $acumulado;
        
        if ($qtde != 0.0) {
            echo ">>>>>>>>>>>>>>>>> handleProdutoSaldo - " . $ordem . PHP_EOL;
            
            $qryGt = $this->dbbonerp->query("SELECT gt.id FROM est_grade_tamanho gt, est_grade g WHERE gt.grade_id = g.id AND g.codigo = ? AND gt.ordem = ?", array(
                $ektProduto['GRADE'],
                $ordem
            ))->result_array();
            
            if (count($qryGt) != 1) {
                die("Erro ao pesquisar grade. Reduzido: [" . $ektProduto['REDUZIDO'] . "]. Código: [" . $ektProduto['GRADE'] . "]. Ordem: [" . $ordem . "]" . PHP_EOL . PHP_EOL . PHP_EOL);
            }
            $gt = $qryGt[0];
            
            $produtoSaldo['produto_id'] = $produto['id'];
            $produtoSaldo['grade_tamanho_id'] = $gt['id'];
            $produtoSaldo['qtde'] = $ektProduto['QT' . $ordemStr] + $acumulado;
            $produtoSaldo['selec'] = $ektProduto['F' . $ordem] == 'S';
            
            $this->produtosaldo_model->save($produtoSaldo) or $this->exit_db_error("Erro ao salvar na est_produto_saldo para o produto id [" . $produto['id'] . "]");
            
            echo ">>>>>>>>>>>>>>>>> OK" . PHP_EOL;
        }
    }

    public function acertaPeriodosReduzidoEKT($produtoBonERP)
    {
        echo ">>>>>>>> ACERTANDO REDUZIDOS EKT: " . $produtoBonERP['reduzido_ekt'] . PHP_EOL;
        
        $corrigiu_algo_aqui = false;
        $produtoId = $produtoBonERP['id'];
        $reduzido_ekt = $produtoBonERP['reduzido_ekt'];
        echo "LIDANDO COM 'depara' [" . $produtoId . "]... \n";
        
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
                $produtoBonERP['reduzido_ekt_ate'] = $this->dtMesano->format('Y-m-d');
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

    public function getQtdeTotalEktProduto($ektProduto)
    {
        $qtdeTotal = ($ektProduto['QT01'] ? $ektProduto['QT01'] : 0.0) + ($ektProduto['QT02'] ? $ektProduto['QT02'] : 0.0) + ($ektProduto['QT03'] ? $ektProduto['QT03'] : 0.0) + ($ektProduto['QT04'] ? $ektProduto['QT04'] : 0.0) + ($ektProduto['QT05'] ? $ektProduto['QT05'] : 0.0) + ($ektProduto['QT06'] ? $ektProduto['QT06'] : 0.0) + ($ektProduto['QT07'] ? $ektProduto['QT07'] : 0.0) + ($ektProduto['QT08'] ? $ektProduto['QT08'] : 0.0) + ($ektProduto['QT09'] ? $ektProduto['QT09'] : 0.0) + ($ektProduto['QT10'] ? $ektProduto['QT10'] : 0.0) + ($ektProduto['QT11'] ? $ektProduto['QT11'] : 0.0) + ($ektProduto['QT12'] ? $ektProduto['QT12'] : 0.0);
        
        return $qtdeTotal;
    }

    public function getQtdeTotalProduto($produto)
    {
        if (! $produto) {
            echo "PRODUTO == NULL ?????? " . PHP_EOL;
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

    public function marcarProdutosInativos()
    {
        // try {
        echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> marcarProdutosInativos" . PHP_EOL . PHP_EOL;
        
        $sql = "SELECT id, reduzido_ekt FROM est_produto WHERE reduzido_ekt_ate IS NULL";
        $qryProds = $this->dbbonerp->query($sql);
        $prods = $qryProds->result_array();
        
        echo "Produtos encontrados: " . count($prods) . PHP_EOL . PHP_EOL;
        
        $i = 0;
        
        // percorre todos os produtos do BonERP
        foreach ($prods as $p) {
            
            $produtoId = $p['id'];
            $reduzidoEkt = $p['reduzido_ekt'];
            
            echo "PESQUISANDO produtoId: " . $produtoId . " - reduzidoEkt: " . $reduzidoEkt . PHP_EOL;
            
            $ektProduto = $ektproduto_model->findByReduzido($reduzidoEkt, $mesImport);
            
            if (! $ektProduto == null) {
                echo "Produto: " . $reduzidoEkt . " não encontrado" . PHP_EOL;
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
        
        echo $i + " produto(s) foram 'inativados'" . PHP_EOL;
    }

    public function deletarSaldos()
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
            die("dtini null" . PHP_EOL . PHP_EOL . PHP_EOL);
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

    public function findDeptoByCodigo($codigo)
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

    public function findSubdeptoByCodigo($codigo)
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

    public function findUnidadeByLabel($label)
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

    public function findGradeByCodigo($codigo)
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

    public function similarity($s1, $s2)
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
        // echo $r;
        return $r;
    }

    /**
     * Tava com muitos errados na tabela est_produto_preco.
     * Dei um TRUNCATE nela e fiz este método para ajustar tudo.
     *
     * @param
     *            $mesano
     */
    public function corrigir_precos($mesano)
    {
        $time_start = microtime(true);
        
        echo "<pre>";
        $this->dbbonerp->trans_start();
        
        $query = $this->dbbonerp->get_where("ekt_produto", array(
            'mesano' => $mesano
        ));
        $result = $query->result_array();
        
        // Pega todos os produtos da ekt_produto para o $mesano
        $i = 1;
        foreach ($result as $r) {
            try {
                // Para cada ekt_produto, encontra o est_produto
                $produto = $this->findByReduzidoEkt($r['REDUZIDO'], $mesano)[0];
                // Adiciona o preço
                echo $i ++ . " (" . $r['id'] . ")\n";
                $this->salvarProdutoPreco($r, $produto['id'], $mesano);
            } catch (Exception $e) {
                print_r($e->getMessage());
                exit();
            }
        }
        
        $this->dbbonerp->trans_complete();
        
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "--------------------------------------------------------------" . PHP_EOL;
        echo "INSERIDOS: " . $this->inseridos . PHP_EOL;
        echo "ATUALIZADOS: " . $this->atualizados . PHP_EOL;
        echo "ACERTADOS DEPARA: " . $this->acertados_depara . PHP_EOL;
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "----------------------------------" . PHP_EOL;
        echo "Total Execution Time: " . $execution_time . "s" . PHP_EOL . PHP_EOL . PHP_EOL;
    }

    public function salvarProdutoPreco($produtoEkt, $produtoId, $mesano)
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
     *
     * @param type $reduzidoEkt
     * @param type $mesano
     * @return type
     * @throws ViewException
     */
    public function findByReduzidoEkt($reduzidoEkt, $mesano = null)
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

    public function gerarProdutoSaldoHistorico($mesano)
    {
        $time_start = microtime(true);
        
        $this->dbbonerp->trans_start();
        
        $this->mesano = $mesano;
        $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . "01");
        if (! $this->dtMesano instanceof DateTime) {
            die("mesano inválido." . PHP_EOL . PHP_EOL . PHP_EOL);
        }
        
        echo "Iniciando gerarProdutoSaldoHistorico() para mesano = '" . $mesano . "'" . PHP_EOL . PHP_EOL;
        
        $this->dbbonerp->query("DELETE FROM est_produto_saldo_historico WHERE DATE_FORMAT(mesano, '%Y%m') = ?", array(
            $mesano
        ));
        
        $ekts = $this->dbekt->query("SELECT
        REDUZIDO,
        coalesce(qt01,0)+coalesce(qt02,0)+coalesce(qt03,0)+coalesce(qt04,0)+coalesce(qt05,0)+coalesce(qt06,0)+
        coalesce(qt07,0)+coalesce(qt08,0)+coalesce(qt09,0)+coalesce(qt10,0)+coalesce(qt11,0)+coalesce(qt12,0) as qtde_total
        FROM
        ekt_produto
        WHERE
        mesano = ?
        ORDER BY reduzido", array(
            $mesano
        ))->result_array();
        
        $total = count($ekts);
        $i = 0;
        
        $r_prods = $this->dbbonerp->query("SELECT reduzido_ekt, produto_id FROM est_produto_reduzidoektmesano WHERE mesano = ?", array(
            $mesano
        ))->result_array();
        
        if (count($r_prods) != $total) {
            die("qtde de produtos diferem" . PHP_EOL . PHP_EOL . PHP_EOL);
        }
        
        $prods = array();
        foreach ($r_prods as $prod) {
            $prods[$prod['reduzido_ekt']] = $prod['produto_id'];
        }
        
        foreach ($ekts as $ekt) {
            echo " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total . PHP_EOL;
            
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
            $mesano
        )) or die("Query fail: " . PHP_EOL . PHP_EOL . PHP_EOL);
        $result = $qry->result_array();
        mysqli_next_result($this->dbbonerp->conn_id);
        $qry->free_result();
        
        if (count($result) == 1) {
            $totalCustos = $result[0]['total_custo'];
            $totalPrecosPrazo = $result[0]['total_precos_prazo'];
            $totalPecas = $result[0]['total_pecas'];
            
            echo "--------------------------------------------------------------" . PHP_EOL;
            echo "Total Custo: " . $totalCustos . PHP_EOL;
            echo "Total Venda: " . $totalPrecosPrazo . PHP_EOL;
            echo "Total Pecas: " . $totalPecas . PHP_EOL;
            echo "--------------------------------------------------------------" . PHP_EOL . PHP_EOL;
            
            $this->handleRegistroConferencia("INVENT PECAS (IMPORTADO)", $this->dtMesano->format('Y-m-t'), $totalPecas);
            $this->handleRegistroConferencia("INVENT CUSTO (IMPORTADO)", $this->dtMesano->format('Y-m-t'), $totalCustos);
            $this->handleRegistroConferencia("INVENT VENDA (IMPORTADO)", $this->dtMesano->format('Y-m-t'), $totalPrecosPrazo);
        }
        
        echo "Finalizando... commitando a transação..." . PHP_EOL;
        
        $this->dbbonerp->trans_complete();
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "----------------------------------" . PHP_EOL;
        echo "Total Execution Time: " . $execution_time . "s" . PHP_EOL . PHP_EOL . PHP_EOL;
    }

    public function handleRegistroConferencia($descricao, $dtMesano, $valor)
    {
        echo "handleRegistroConferencia - [" . $descricao . "]" . PHP_EOL;
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
        echo "OK!!!" . PHP_EOL;
    }

    private function exit_db_error($msg = null)
    {
        echo str_pad("", 100, "*") . "\n";
        echo $msg ? $msg . PHP_EOL : '';
        echo "LAST QUERY: " . $this->dbbonerp->last_query() . PHP_EOL . PHP_EOL;
        print_r($this->dbbonerp->error()) . PHP_EOL . PHP_EOL;
        echo str_pad("", 100, "*") . PHP_EOL;
        exit();
    }

    public function teste($mesano)
    {
        $results = $this->findByReduzidoEkt(1234, '201702');
        print_r($results);
    }

    public function corrigirReduzidos()
    {
        $time_start = microtime(true);
        
        echo "Iniciando a correção de reduzidos..." . PHP_EOL;
        
        $this->dbbonerp->trans_start();
        
        $r = $this->dbbonerp->query("SELECT id, reduzido, reduzido_ekt FROM est_produto")->result_array();
        $total = count($r);
        $i = 0;
        foreach ($r as $produto) {
            echo " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " . ++ $i . "/" . $total . PHP_EOL;
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
        echo "Finalizando... commitando a transação..." . PHP_EOL;
        
        $this->dbbonerp->trans_complete();
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo PHP_EOL . PHP_EOL . PHP_EOL;
        echo "----------------------------------" . PHP_EOL;
        echo "Total Execution Time: " . $execution_time . "s" . PHP_EOL . PHP_EOL . PHP_EOL;
    }
}