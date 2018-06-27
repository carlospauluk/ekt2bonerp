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
    private $dtMesAno;

    private $produtosaldo_model;
    
    private $inseridos;
    private $existentes;
    

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        $this->load->database();
        $this->load->library('datetime_library');
        error_reporting(E_ALL);
        ini_set('display_errors', 1);
        
        $this->agora = new DateTime();
        
        $this->load->model('est/produto_model');
        $this->load->model('est/fornecedor_model');
        $this->load->model('ekt/ektproduto_model');
        
        $this->produtosaldo_model = new \CIBases\Models\DAO\Base\Base_model('est_produto_saldo');
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
        
        $this->db->trans_start();
        
        echo PHP_EOL . PHP_EOL;
        
        $this->csvsPath = getenv('EKT_CSVS_PATH') or die("EKT_CSVS_PATH não informado\n\n\n");
        $this->logPath = getenv('EKT_LOG_PATH') or die("EKT_LOG_PATH não informado\n\n\n");
        
        echo "csvsPath: [" . $this->csvsPath . "]" . PHP_EOL;
        echo "logPath: [" . $this->logPath . "]" . PHP_EOL;
        $this->logFile = $this->logPath . "espelhos2bonerp-PROD-" . $this->agora->format('Y-m-d_H-i-s') . ".txt";
        echo "logFile: [" . $this->logFile . "]" . PHP_EOL;
        
        if ($acao == 'PROD') {
            echo "Iniciando a importação para o mês/ano: [" . $mesano . "]" . PHP_EOL;
            $this->mesano = $mesano;
            $this->dtMesAno = DateTime::createFromFormat('Ymd', $mesano . "01");
            if (! $this->dtMesAno instanceof DateTime) {
                die("mesano inválido.\n\n\n");
            }
            $this->deletarSaldos();
            $this->importarProdutos();
        }
        
        if ($acao == 'DEATE') {
            $this->acertarDeAteProdutos();
        }
        
        $this->db->trans_complete();
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s\n\n";
    }

    public function importarProdutos()
    {
        $model = $this->ektproduto_model;
        
        $l = $model->findByMesano($this->mesano);
        
        foreach ($l as $ektProduto) {
            $this->importarProduto($ektProduto);
        }
    }

    public function importarProduto($ektProduto)
    {
        $this->load->model('est/produto_model');
        
        // Verifica produtos com mesmo reduzidoEKT
        $produtosComMesmoReduzidoEKT = $this->produto_model->findByReduzidoEkt($ektProduto['REDUZIDO'], null);
        
        $c = 0;
        $a = 0;
        
        if (count($produtosComMesmoReduzidoEKT) == 0) {
            $this->saveProduto($ektProduto, null);
            $c ++;
        } else {
            $achouMesmo = false;
            foreach ($produtosComMesmoReduzidoEKT as $mesmoReduzido) {
                
                $descricao_ekt = trim($ektProduto['DESCRICAO']);
                $descricao = trim($mesmoReduzido['descricao']);
                // similar_text($descricao_ekt, $descricao, $percent);
                // $percent = $this->similarity($descricao_ekt,$descricao);
                
                //
                // if ( $percent >= 0.75 or (! $mesmoReduzido['reduzido_ekt_ate']) and $mesmoReduzido['reduzido_ekt_desde'] == $this->dtMesImport) {
                if ($descricao_ekt == $descricao) {
                    // PRODUTO JÁ EXISTENTE
                    $achouMesmo = true;
                    $this->saveProduto($ektProduto, $mesmoReduzido);
                    $this->saveGrade($ektProduto, $mesmoReduzido);
                    $a ++;
                    
                    $qtdeTotal_ektProduto = $this->getQtdeTotalEktProduto($ektProduto);
                    $qtdeTotal_produto = $this->getQtdeTotalProduto($produto);
                    
                    if ($qtdeTotal_ektProduto != $qtdeTotal_produto) {
                        // logger.info("************ ATENÇÃO: qtdes divergindo");
                    }
                    
                    $this->acertaPeriodosReduzidoEKT($ektProduto);
                    break; // já achou, não precisa continuar procurando
                }
            }
            if (! $achouMesmo) {
                $produto = $this->saveProduto($ektProduto, null);
                $this->saveGrade($ektProduto, $produto);
                $this->acertaPeriodosReduzidoEKT($ektProduto);
                // $this->updateQtdeTotal($produto);
            }
        }
    }

    public function saveProduto($ektProduto, $produto = null)
    {
        // try {
        echo " ________________________ CADASTRANDO NOVO PRODUTO " . PHP_EOL . PHP_EOL;
        
        $produto['depto_imp_id'] = $this->findDeptoBycodigo($ektProduto['DEPTO']);
        $produto['subdepto_id'] = $this->findSubdeptoBycodigo($ektProduto['SUBDEPTO']);
        $produto['subdepto_err'] = $this->findSubdeptoBycodigo($ektProduto['SUBDEPTO']);
        $fornecedor_id = $this->fornecedor_model->findByCodigoEkt($ektProduto['FORNEC'], $this->dtMesAno);
        $produto['fornecedor_id'] = $fornecedor_id;
        
        $produto['descricao'] = $ektProduto['DESCRICAO'];
        $produto['dt_ult_venda'] = $ektProduto['DATA_ULT_VENDA'];
        $produto['grade_id'] = $this->findGradeByCodigo($ektProduto['GRADE']);
        $produto['grade_err'] = $ektProduto['GRADE'];
        
        if ($produto['reduzido']) {
            if (substr($produto['reduzido'], 4) != str_pad($ektProduto['REDUZIDO'], 6, '0', STR_PAD_LEFT)) {
                die("Problema com reduzido... bonerp: [" . $produto['reduzido'] . "]. EKT: [" . $ektProduto['REDUZIDO'] . "]");
            }
        } else {
            $reduzido = $this->dtMesAno->format('ym') . str_pad($ektProduto['REDUZIDO'], 6, '0', STR_PAD_LEFT);
            $produto['reduzido'] = $reduzido;
        }
        
        $produto['reduzido_ekt'] = $ektProduto['REDUZIDO'];
        
        if (! $produto['reduzido_ekt_desde']) {
            $produto['reduzido_ekt_desde'] = $this->dtMesAno->format('Y-m') . "01";
        }
        
        $produto['referencia'] = $ektProduto['REFERENCIA'];
        
        $produto['unidade_produto_id'] = $this->findUnidadeByLabel($ektProduto['UNIDADE']);
        $produto['unidade_produto_err'] = $ektProduto['UNIDADE'];
        
        $produto['cst'] = 102;
        $produto['icms'] = 0;
        $produto['tipo_tributacao'] = "T";
        $produto['ncm'] = $ektProduto['NCM'] ? $ektProduto['NCM'] : "62179000";
        $produto['fracionado'] = $ektProduto['FRACIONADO'] == 'S' ? true : false;
        
        echo " ________________________ save no CADASTRANDO NOVO PRODUTO " . PHP_EOL;
        $produto_id = $this->produto_model->save($produto);
        $produto['id'] = $produto_id;
        echo " ________________________ OK " . PHP_EOL;
        
        // Se é uma atualização de produto, verifica se o preço foi alterado
        if ($produto['id']) {
            
            $params = array();
            $params[] = $produto_id;
            // $params[] = $ektProduto['COEF'];
            // $params[] = $ektProduto['MARGEMC'];
            $params[] = $ektProduto['DATA_PCUSTO'];
            // $params[] = $ektProduto['DATA_PVENDA'];
            // $params[] = $ektProduto['MARGEM'];
            // $params[] = $ektProduto['PRAZO'];
            $params[] = $ektProduto['PCUSTO'];
            $params[] = $ektProduto['PPRAZO'];
            // $params[] = $ektProduto['PPROMO'];
            // $params[] = $ektProduto['PVISTA'];
            // $params[] = 0.15;
            
            // Se alterou qualquer coisa, já salva um novo preço
            //@formatter:off
            $sql = "SELECT 1 FROM est_produto_preco WHERE " 
                    . "produto_id = ? AND " . 
//                     "coeficiente = ? AND " . 
//                     "custo_operacional = ? AND " . 
                    "dt_custo = ? AND " . 
//                     "dt_preco_venda = ? AND " . 
//                     "margem = ? AND " . 
//                     "prazo = ? AND " . 
                    "preco_custo = ? AND " . 
                    "preco_prazo = ? AND " . 
//                     "preco_promo = ? AND " . 
//                     "preco_vista  = ? AND " . 
//                     "custo_financeiro = ? AND " .
                    "1=1";
            // @formatter:on
            
            $mesmo = $this->db->query($sql, $params)->result_array();
            
            if (! $mesmo) {
                $this->savePreco($ektProduto, $produto_id);
            }
        } else {
            $this->savePreco($ektProduto, $produto_id);
        }
        
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
        
        $preco_model = new \CIBases\Models\DAO\Base\Base_model('est_produto_preco');
        
        echo " ________________________ salvando PRECO " . PHP_EOL;
        $preco_model->save($preco);
        echo " ________________________ OK " . PHP_EOL;
    }

    public function checkProdutosIguais($mesmoReduzido, $ektProduto)
    {}

    /**
     */
    public function saveGrade($ektProduto, $produto)
    {
        echo ">>>>>>>>>>>>>>>>> SALVANDO GRADE PRODUTO: " . $ektProduto . PHP_EOL;
        
        $saldos = $this->db->query("SELECT count(*) as qtde FROM est_produto_saldo WHERE produto_id = ?", array(
            $produto['id']
        ))->result_array();
        
        if ($saldos['qtde'] > 0) {
            throw new \Exception("Já tem saldo: " + $produto['descricao']);
        }
        
        $qryQtdeTamanhos = $this->db->query("SELECT count(*) as qtde FROM est_grade_tamanho WHERE grade_id = ?", array(
            $produto['grade_id']
        ))->result_array();
        if (! $qryQtdeTamanhos[0] or ! $qryQtdeTamanhos[0]['qtde']) {
            die("Erro ao pesquisar tamanhos para a grade " . $produto['grade_id']);
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
        
        for ($i = 1; $i <= 12; $i ++) {
            $this->handleProdutoSaldo($ektProduto, $produto, $i, $acumulado);
            $acumulado = 0.0;
        }
        
        echo ">>>>>>>>>>>>>>>>> OK ";
        
        return $produto;
    }

    public function handleProdutoSaldo($ektProduto, $produto, $ordem, $acumulado)
    {
        $ordemStr = str_pad($ordem, 2, '0', STR_PAD_LEFT);
        
        if ($ektProduto['QT' . $ordemStr]) {
            echo ">>>>>>>>>>>>>>>>> handleProdutoSaldo - " . $ordem . PHP_EOL;
            
            $qryGt = $this->db->query("SELECT gt.id FROM est_grade_tamanho gt, est_grade g WHERE gt.grade_id = g.id AND g.codigo = ? AND gt.ordem = ?", array(
                $ektProduto['GRADE'],
                $ordem
            ))->result_array();
            $gt = $qryGt[0];
            
            if ($gt == null) {
                throw new \Exception("GradeTamanho null: " . $ordem);
            }
            
            $produtoSaldo['produto_id'] = $produto['id'];
            $produtoSaldo['grade_tamanho_id'] = $gt['id'];
            $produtoSaldo['qtde'] = $ektProduto['QT' . $ordemStr] + $acumulado;
            $produtoSaldo['selec'] = $ektProduto['F' . $ordem] == 'S';
            
            $this->produtosaldo_model->save($produtoSaldo);
            
            echo ">>>>>>>>>>>>>>>>> OK" . PHP_EOL;
        }
    }

    public function acertaPeriodosReduzidoEKT($ektProduto)
    {
        // $diff = $this->agora->diff($this->dtMesAno)->days;
        // if ($diff != 0) {
        // // não dá pra acertar periodos do EKT quando importando de meses anteriores, senão dá cagada...
        // return;
        // }
        echo ">>>>>>>> ACERTANDO REDUZIDOS EKT: " + $ektProduto['REDUZIDO'] . PHP_EOL;
        // Pega todos os produtos que tenham o mesmo reduzido EKT
        
        // Pega todos os produtos com o mesmo reduzido
        $produtosComMesmoReduzidoEKT = $this->produto_model->findByReduzidoEkt($ektProduto['REDUZIDO'], null);
        
        // força o reduzidoEktAte do último para null
        $ultimo = $produtosComMesmoReduzidoEKT[count($produtosComMesmoReduzidoEKT) - 1];
        $ultimo['reduzido_ekt_ate'] = null;
        echo "Salvando o último" . PHP_EOL;
        $this->produto_model->save($ultimo);
        
        // seta o reduzido_ekt_ate do 'penúltimo' como 1 dia antes do reduzido_ekt_desde do 'último'
        $penultimo = $produtosComMesmoReduzidoEKT[count($produtosComMesmoReduzidoEKT) - 2];
        
        $dtDesde = \DateTime::createFromFormat('Y-m-d', $ultimo['reduzido_ekt_desde']);
        $dtDesde->sub(new \DateInterval('1 day'));
        $penultimo['reduzido_ekt_ate'] = $dtDesde->format('Y-m-d');
        
        $this->produto_model->save($penultimo);
        
        // Pesquisa de novo
        $produtosComMesmoReduzidoEKT = $this->produto_model->findByReduzidoEkt($ektProduto['REDUZIDO'], null);
        
        // Só pode ter 1 com reduzido_ekt_ate
        $qtdeComAteNull = 0;
        foreach ($produtosComMesmoReduzidoEKT as $produto) {
            if (! $produto['reduzido_ekt_ate']) {
                $qtdeComAteNull ++;
            }
        }
        if ($qtdeComAteNull > 1) {
            throw new Exception("Mais de dois produto com reduzidoEktAte nulo");
        }
        
        echo ">>>>>>>>>>>> REDUZIDO EKT ATE CORRIGIDO" . PHP_EOL;
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
        echo " >>>> updateQtdeTotal(Produto produto = '" + $produto['id'] + "')" . PHP_EOL;
        
        $qtdeTotal = 0;
        
        $saldos = $this->db->query("SELECT qtde FROM est_produto_saldo WHERE produto_id = ?", array(
            $produto['id']
        ));
        
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
        $qryProds = $this->db->query($sql);
        $prods = $qryProds->result_array();
        
        echo "Produtos encontrados: " + count($prods) . PHP_EOL . PHP_EOL;
        
        $i = 0;
        
        // percorre todos os produtos do BonERP
        foreach ($prods as $p) {
            
            $produtoId = $p['id'];
            $reduzidoEkt = $p['reduzido_ekt'];
            
            echo "PESQUISANDO produtoId: " + $produtoId + " - reduzidoEkt: " + $reduzidoEkt . PHP_EOL;
            
            $ektProduto = $ektproduto_model->findByReduzido($reduzidoEkt, $mesImport);
            
            if (! $ektProduto == null) {
                echo "Produto: " + $reduzidoEkt + " não encontrado" . PHP_EOL;
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
        $this->db->query($sql);
    }

//     public function acertarDeAteProdutos()
//     {
//         $time_start = microtime(true);
        
//         $this->db->trans_start();
        
//         echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> acertarDeAteProdutos" . PHP_EOL . PHP_EOL;
//         // logger.info(sdfMesAno.format(dtMesImport));
        
//         // Encontro todos os produtos que sejam do mesmo REDUZIDO_EKT e que nenhum deles tenha reduzido_ekt_ate = NULL
//         $sql = "SELECT id, REDUZIDO, DESCRICAO, mesano FROM ekt_produto ORDER BY REDUZIDO, mesano"; // WHERE reduzido_ekt = 2083";
//         $qry = $this->db->query($sql);
//         $ekts = $qry->result_array();
        
//         // logger.info("Produtos sem 'reduzido_ekt_ate' encontrados: " + reduzidosEkt.size());
        
//         // $bonERPs = $this->db->query("SELECT id, descricao, reduzido_ekt, reduzido_ekt_desde, reduzido_ekt_ate FROM est_produto ORDER BY reduzido_ekt, reduzido_ekt_desde")->result_array();
        
//         // percorre todos os produtos do EKT
        
//         $desde = null;
//         for ($i = 0; $i < count($ekts) - 1; $i ++) {
            
//             $atual = $ekts[$i];
//             $prox = $ekts[$i + 1];
            
//             if (! $desde) {
//                 $desde = $atual['mesano'];
//             }
            
//             // Verifica se o próximo já é outro reduzido
//             if ($atual['REDUZIDO'] != $prox['REDUZIDO']) {
//                 // Se mudou, então tem que achar um est_produto que seja com reduzido_ekt_ate = null
//                 $params = array();
//                 $params[] = $atual['REDUZIDO'];
//                 $dtDesde = DateTime::createFromFormat('Ym', $desde);
//                 $params[] = $dtDesde->format('Y-m-') . "01";
//                 $r = $this->db->query("SELECT id, descricao, reduzido_ekt_desde, reduzido_ekt_ate FROM est_produto WHERE reduzido_ekt = ? AND reduzido_ekt_desde = ? AND reduzido_ekt_ate IS NULL", $params)->result_array();
//                 if (count($r) != 1) {
//                     $this->corrigirDeAteProduto($atual);
//                 }
//                 $desde = null;
//             } else {
//                 $desc_atual = trim($atual['DESCRICAO']);
//                 $desc_prox = trim($prox['DESCRICAO']);
//                 // similar_text($desc_atual, $desc_prox, $percent);
//                 $percent = $this->similarity($desc_atual, $desc_prox);
                
//                 // Se mais que 75%, considera que mudou o produto
//                 if ($percent < 0.75) {
//                     $params = array();
//                     $params[] = $atual['REDUZIDO'];
//                     $dtDesde = DateTime::createFromFormat('Ym', $desde);
//                     $params[] = $dtDesde->format('Y-m-') . "01";
                    
//                     $dtDesde_prox = DateTime::createFromFormat('Ym', $prox['mesano']);
//                     $params[] = $dtDesde_prox->format('Y-m-') . "01";
                    
//                     $r = $this->db->query("SELECT id, descricao, reduzido_ekt_desde, reduzido_ekt_ate FROM est_produto WHERE reduzido_ekt = ? AND reduzido_ekt_desde = ? AND reduzido_ekt_ate < ?", $params)->result_array();
//                     if (count($r) != 1) {
//                         $this->corrigirDeAteProduto($atual);
//                     }
//                     $desde = null;
//                 }
//             }
//         }
//         $this->db->trans_complete();
        
//         $time_end = microtime(true);
//         $execution_time = ($time_end - $time_start);
//         echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s\n\n";
//     }

//     private function corrigirDeAteProduto($produtoEkt)
//     {
//         $reduzidoEkt = $produtoEkt['REDUZIDO'];
//         $params = array(
//             $reduzidoEkt
//         );
        
//         $ekts = $this->db->query("SELECT id, REDUZIDO, DESCRICAO, mesano FROM ekt_produto WHERE REDUZIDO = ? ORDER BY mesano", $params)->result_array();
        
//         $prods = $this->db->query("SELECT id, descricao, reduzido_ekt_desde, reduzido_ekt_ate FROM est_produto WHERE reduzido_ekt = ? ORDER BY reduzido_ekt_desde", $params)->result_array();
        
//         for ($p = 0; $p < count($prods) - 1; $p ++) {
            
//             $prod_atual = $prods[$p];
//             $prod_prox = $prods[$p + 1];
            
//             $mesesanos = $this->getMesAnoList($prod['reduzido_ekt_desde'], $prod['reduzido_ekt_ate']);
//             for ($i = 0; $i < count($ekts) - 1; $i ++) {
//                 $atual = $ekts[$i];
//                 $prox = $ekts[$i + 1];
                
//                 $desc_atual = trim($atual['DESCRICAO']);
//                 $desc_prox = trim($prox['DESCRICAO']);
//                 $percent = $this->similarity($desc_atual, $desc_prox);
                
//                 if ($desc_atual != $desc_prox) {
//                     echo "";
//                 }
                
//                 $dtAte = DateTime::createFromFormat('Y-m-d', $prod_atual['reduzido_ekt_ate']);
//                 $ate = $dtAte->format('Ym');
                
//                 // deve ser o último
//                 if ($percent < 0.75) {
//                     if ($ate != $atual['mesano']) {
//                         die("errado");
//                     }
//                 }
                
//                 if ($atual['mesano'] == $ate) {
//                     if ($percent != 100.0) {
//                         die("errado?");
//                     }
//                     $dtProxDesde = DateTime::createFromFormat('Y-m-d', $prox_prox['reduzido_ekt_desde']);
//                     $proxdesde = $dtProxDesde->format('Ym');
//                     if ($proxdesde != $prox['mesano']) {
//                         die("errado");
//                     }
//                 }
//             }
            
//             // conferir o último
//             if ($prod_atual['reduzido_ekt_ate'] == null) {
//                 if ($i < count($ekts) - 1) {
//                     die("erro, tem mais");
//                 }
//             }
            
//             exit();
//         }
//     }

    private function getMesAnoList($dtIni, $dtFim)
    {
        $dtIni = DateTime::createFromFormat('Y-m-d', $dtIni);
        $dtFim = DateTime::createFromFormat('Y-m-d', $dtFim);
        if ($dtFim < $dtIni) {
            return;
        }
        if ($dtIni == null) {
            die("dtini null");
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

//     private function corrigirDeAteProduto($produtoEkt)
//     {
//         // Percorro toda a lista do ekt_produto
//         $params = array(
//             $produtoEkt['REDUZIDO']
//         );
//         $ekts = $this->db->query("SELECT id, REDUZIDO, DESCRICAO, mesano FROM ekt_produto WHERE REDUZIDO = ? ORDER BY mesano", $params)->result_array();
        
//         // Confiro quantos produtos diferentes tem com o mesmo reduzido.
//         $qtdeDiferentes = 1;
//         for ($i = 0; $i < count($ekts) - 1; $i ++) {
            
//             $atual = $ekts[$i];
//             $prox = $ekts[$i + 1];
            
//             $desc_atual = trim($atual['DESCRICAO']);
//             $desc_prox = trim($prox['DESCRICAO']);
//             // similar_text($desc_atual, $desc_prox, $percent);
//             $percent = $this->similarity($desc_atual, $desc_prox);
            
//             if ($desc_atual != $desc_prox) {
//                 echo "";
//             }
            
//             // Se mais que 75%, considera que mudou o produto
//             if ($percent < 0.75) {
//                 $qtdeDiferentes ++;
//             }
//         }
        
//         // Tem que ter a mesma quantidade no est_produto.
//         $params = array(
//             $produtoEkt['REDUZIDO']
//         );
//         $results = $this->db->query("SELECT id, descricao, reduzido_ekt_desde, reduzido_ekt_ate FROM est_produto WHERE reduzido_ekt = ? ORDER BY reduzido_ekt_desde", $params)->result_array();
//         if (count($results) != $qtdeDiferentes) {
//             die("Qtde de produtos difere entre est_produto e ekt_produto. Reduzido: " . $produtoEkt['REDUZIDO']);
//         } else {
//             $r = 0;
//             $desde = null;
//             // Percorrendo a lista inteira dos ekt_produto
//             for ($i = 0; $i < count($ekts) - 1; $i ++) {
                
//                 $atual = $ekts[$i];
//                 $prox = $ekts[$i + 1];
//                 // Marco qual será o ekt_reduzido_desde
//                 if (! $desde) {
//                     $desde = $atual['mesano'];
//                 }
                
//                 $desc_atual = trim($atual['DESCRICAO']);
//                 $desc_prox = trim($prox['DESCRICAO']);
//                 // similar_text($desc_atual, $desc_prox, $percent);
//                 $percent = $this->similarity($desc_atual, $desc_prox);
                
//                 // Se mais que 75%, considera que mudou o produto
//                 if ($percent < 0.75) {
//                     $prod = $results[$r];
//                     // Arrumo o desde
//                     $dtDesde = DateTime::createFromFormat('Ym', $desde);
//                     $dtAtualMesano = DateTime::createFromFormat('Ym', $atual['mesano']);
//                     $prod['reduzido_ekt_desde'] = $dtDesde->format('Y-m-') . "01";
//                     // Arrumo o ate
//                     $prod['reduzido_ekt_ate'] = $dtAtualMesano->format('Y-m-t');
//                     // Salvo esse produto
//                     $this->produto_model->save($prod);
                    
//                     // Arrumo o desde do próximo
//                     $dtDesdeProx = DateTime::createFromFormat('Ym', $prox['mesano']);
//                     $proxProd = $results[$r + 1];
//                     $proxProd['reduzido_ekt_desde'] = $dtDesdeProx->format('Y-m-') . "01";
                    
//                     // Salvo o próximo, mantendo o ate do próximo em aberto
//                     $this->produto_model->save($proxProd);
//                 }
//             }
            
//             // O último sempre tem que tá com o ate = null
//             $ultimo = $results[count($results) - 1];
//             if ($ultimo['reduzido_ekt_ate'] !== null) {
//                 $ultimo['reduzido_ekt_ate'] = null;
//                 $this->produto_model->save($ultimo);
//             }
//         }
//     }

    private $deptos;

    public function findDeptoByCodigo($codigo)
    {
        if (! $this->deptos) {
            $this->deptos = array();
            $sql = "SELECT id, codigo FROM est_depto";
            $r = $this->db->query($sql)->result_array();
            foreach ($r as $depto) {
                $this->deptos[$depto['codigo']] = $depto['id'];
            }
        }
        // Se não achar, retorna o 999999 (ERRO DE IMPORTAÇÃO)
        return $this->deptos[$codigo] ? $this->deptos[$codigo] : $this->deptos['999999'];
    }

    private $subdeptos;

    public function findSubdeptoByCodigo($codigo)
    {
        if (! $this->subdeptos) {
            $this->subdeptos = array();
            $sql = "SELECT id, codigo FROM est_subdepto";
            $r = $this->db->query($sql)->result_array();
            foreach ($r as $subdepto) {
                $this->subdeptos[$subdepto['codigo']] = $subdepto['id'];
            }
        }
        // Se não achar, retorna o 999999 (ERRO DE IMPORTAÇÃO)
        return $this->subdeptos[$codigo] ? $this->subdeptos[$codigo] : $this->subdeptos['999999'];
    }

    private $unidades;

    public function findUnidadeByLabel($label)
    {
        if (! $this->unidades) {
            $this->unidades = array();
            $sql = "SELECT id, label FROM est_unidade_produto";
            $r = $this->db->query($sql)->result_array();
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
            $r = $this->db->query($sql)->result_array();
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
     * @param type $mesano
     */
    public function corrigir_precos($mesano)
    {
        $time_start = microtime(true);
        
        echo "<pre>";
        $this->db->trans_start();
        
        $query = $this->db->get_where("ekt_produto", array(
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
        
        $this->db->trans_complete();
        
        echo "\n\n\nINSERIDOS: " . $this->inseridos;
        echo "\nEXISTENTES: " . $this->existentes;
        
        $time_end = microtime(true);
        
        $execution_time = ($time_end - $time_start);
        
        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s";
    }

    public function salvarProdutoPreco($produtoEkt, $produtoId, $mesano)
    {
        if (! $produtoEkt['DATA_PCUSTO']) {
            $produtoEkt['DATA_PCUSTO'] = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
        }
        
        if (! $produtoEkt['DATA_PVENDA']) {
            $produtoEkt['DATA_PVENDA'] = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
        }
        
        $query = $this->db->get_where('est_produto_preco', array(
            'produto_id' => $produtoId,
            'dt_custo' => $produtoEkt['DATA_PCUSTO'],
            'preco_custo' => $produtoEkt['PCUSTO'],
            'preco_prazo' => $produtoEkt['PPRAZO']
        ));
        $existe = $query->result_array();
        // echo "COMANDO: " . $this->db->last_query() . "\n";
        // print_r($existe);
        // echo "\n\nCONT: " . count($existe) . "\n";
        // exit;
        if (count($existe) > 0) {
            $this->existentes ++;
            return;
        }
        
        $dtMesAno = DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
        
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
            'mesano' => $dtMesAno
        );
        
        $this->db->insert('est_produto_preco', $data) or exit_db_error();
        
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
        
        $query = $this->db->query($sql, $params);
        $result = $query->result_array();
        
        if ($mesano && count($result) > 1) {
            throw new Exception("Mais de um produto com o mesmo reduzido ('$reduzidoEkt) no período ('$mesano')");
        } else {
            return $result;
        }
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
}