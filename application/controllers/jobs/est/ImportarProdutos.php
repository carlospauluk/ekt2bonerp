<?php


require_once('./application/libraries/file/LogWriter.php');
require_once('./application/libraries/util/Datetime_utils.php');

/**
 * Classe responsável por importar produtos das tabelas espelho (ekt_*) para o crosier.
 *
 * php index.php jobs/est/ImportarProdutos importar PROD YYYYMM
 *
 * @author Carlos Eduardo Pauluk
 *
 */
class ImportarProdutos extends CI_Controller
{

    /**
     * @var LogWriter
     */
    private $logger;

    private $agora;

    /**
     * Passado pela linha de comando no formato YYYYMM.
     *
     * @var string
     */
    private $mesano;

    /**
     * Parseado do $mesano para um DateTime.
     *
     * @var DateTime
     */
    private $dtMesano;

    /**
     * Se o $mesano = now
     * @var
     */
    private $importandoMesCorrente;

    private $inseridos;

    private $atualizados;

    /**
     * @var CI_DB_mysqli_driver
     */
    private $dbekt;

    /**
     * @var CI_DB_mysqli_driver
     */
    private $dbcrosier;

    private $subgrupos;

    private $unidades;

    private $atributos;


    /**
     * @var \CIBases\Models\DAO\Base\Base_model
     */
    public $produtosaldo_model;

    /**
     * @var Produto_model
     */
    public $produto_model;

    /**
     * @var \CIBases\Models\DAO\Base\Base_model
     */
    public $preco_model;

    /**
     * @var Fornecedor_model
     */
    public $fornecedor_model;

    /**
     * @var Ektproduto_model
     */
    public $ektproduto_model;

    /**
     * @var \CIBases\Models\DAO\Base\Base_model
     */
    public $produtoatributo_model;


    /**
     * ImportarProdutos constructor.
     * @throws Exception
     */
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

        $this->preco_model = new \CIBases\Models\DAO\Base\Base_model('est_produto_preco', 'crosier');
        $this->preco_model->setDb($this->dbcrosier);

        $this->load->model('est/fornecedor_model');
        $this->fornecedor_model->setDb($this->dbcrosier);

        $this->load->model('ekt/ektproduto_model');
        $this->ektproduto_model->setDb($this->dbekt);

        $this->produtosaldo_model = new \CIBases\Models\DAO\Base\Base_model('est_produto_saldo', 'crosier');
        $this->produtosaldo_model->setDb($this->dbcrosier);

        $this->produtoatributo_model = new \CIBases\Models\DAO\Base\Base_model('est_produto_atributo', 'crosier');
        $this->produtoatributo_model->setDb($this->dbcrosier);
    }

    /**
     * Método principal.
     *
     * @param $mesano (yyyymm)
     * @param $acao (PROD,
     *            DEATE)
     * @throws Exception
     */
    public function importar($acao, $mesano = null): void
    {
        $time_start = microtime(true);

        $logPath = getenv('EKT_LOG_PATH') ?: './log/';
        $prefix = 'ImportarProdutos' . '_' . $mesano . '_' . $acao . '_';
        $this->logger = new LogWriter($logPath, $prefix);

        $this->logger->info('logPath: [' . $logPath . ']');

        $mesano = $mesano ? $mesano : (new DateTime())->format('Ym');
        $this->mesano = $mesano;
        $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . '01');
        if ($this->dtMesano && !$this->dtMesano instanceof DateTime) {
            $this->logger->info('mesano inválido.');
            $this->logger->sendMail();
            $this->logger->closeLog();
            exit();
        }

        $this->importandoMesCorrente = $this->mesano === $this->agora->format('Ym');
        $this->logger->info("Importando 'atual'? " . ($this->importandoMesCorrente ? 'SIM' : 'NÃO'));

        if ($acao === 'PROD') {
            $this->dtMesano->setTime(0, 0, 0, 0);
            $this->deletarSaldos();
            $this->importarProdutos();
            $this->gerarProdutoSaldoHistorico();

            if ($this->importandoMesCorrente) {
                // Se está importando para o mês corrente, corrige os campos reduzido_ekt_ate
                $this->corrigeReduzidoEktAteMesCorrente();
            }
            $this->corrigirCampoAtual();
        }
        if ($acao === 'DEATE') {
            $this->corrigirProdutosReduzidoEktDesdeAte(); // apaga tudo da est_produto_reduzidoektmesano
            $this->corrigeReduzidoEktDesdeAte(); // corrige os reduzido_ekt_desde e reduzido_ekt_ate
            $this->corrigirCampoAtual(); // acerta o campo est_produto.atual
        }
        if ($acao === 'DESDES_ATES') {
            $this->corrigeReduzidoEktDesdeAte(); // corrige apenas os atributos reduzido_ekt_desde e reduzido_ekt_ate
        }
        if ($acao === 'PRECOS') {
            $this->corrigirPrecos();
        }

        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        $this->logger->info(PHP_EOL);
        $this->logger->info('----------------------------------');
        $this->logger->info('Tempo total: ' . $execution_time . 's');
        $this->logger->sendMail();
        $this->logger->closeLog();
    }

    /**
     */
    private function deletarSaldos(): void
    {
        $this->dbcrosier->query('DELETE FROM est_produto_reduzidoektmesano WHERE mesano = ?', [$this->mesano]);
        $this->dbcrosier->query('SET FOREIGN_KEY_CHECKS = 0') or $this->exit_db_error('Erro ao SET FOREIGN_KEY_CHECKS = 0');
        $this->dbcrosier->query('TRUNCATE TABLE est_produto_saldo_atributo') or $this->exit_db_error('Erro ao TRUNCATE TABLE est_produto_saldo_atributo');
        $this->dbcrosier->query('TRUNCATE TABLE est_produto_saldo') or $this->exit_db_error('Erro ao TRUNCATE TABLE est_produto_saldo');
        $this->dbcrosier->query('SET FOREIGN_KEY_CHECKS = 1') or $this->exit_db_error('Erro ao SET FOREIGN_KEY_CHECKS = 1');
    }

    /**
     * @param null $msg
     */
    private function exit_db_error($msg = null): void
    {
        $this->logger->info(str_pad('', 100, '*'));
        $this->logger->info($msg ? $msg : '');
        $this->logger->info('LAST QUERY: ' . $this->dbcrosier->last_query());
        print_r($this->dbcrosier->error());
        $this->logger->info(str_pad('', 100, '*'));
        exit();
    }

    /**
     * @throws Exception
     */
    private function importarProdutos(): void
    {
        $this->logger->info('Iniciando a importação de produtos...');
        $this->dbcrosier->trans_start();

        $l = $this->ektproduto_model->findByMesano($this->mesano);
        // $l = $this->dbekt->query("SELECT * FROM ekt_produto WHERE reduzido = 1507 AND mesano = ?", array($this->mesano))->result_array();

        $total = count($l);
        $this->logger->info(' >>>>>>>>>>>>>>>>>>>> ' . $total . ' produto(s) encontrado(s).');

        $i = 0;
        foreach ($l as $ektProduto) {
            if ($ektProduto['REDUZIDO'] === 88888) {
                continue;
            }
            if (trim($ektProduto['DESCRICAO']) === '') {
                $this->logger->info(" >>>>>>>>>>>>>>>>>>>> PRODUTO com reduzido = '" . $ektProduto['REDUZIDO'] . ' está sem descrição. PULANDO.');
                continue;
            }
            $this->logger->info('>>> ' . str_pad($ektProduto['REDUZIDO'], 6, '0', STR_PAD_LEFT) . ' - [' . $ektProduto['DESCRICAO'] . '] ................... ' . str_pad(++$i, 6, '0', STR_PAD_LEFT) . '/' . str_pad($total, 6, '0', STR_PAD_LEFT));
            $this->importarProduto($ektProduto);
        }
        $this->logger->info('Finalizando... commitando a transação...');
        $this->dbcrosier->trans_complete();
        $this->logger->info('OK!!!');
    }

    /**
     * Verifica se é um novo produto
     * @param $ektProduto
     * @throws Exception
     */
    private function importarProduto($ektProduto): void
    {
        // Verifica produtos com mesmo reduzidoEKT
        $produtosComMesmoReduzidoEKT = $this->produto_model->findByReduzidoEkt($ektProduto['REDUZIDO']);

        $qtdeComMesmoReduzido = count($produtosComMesmoReduzidoEKT);

        // Se não tem nenhum produto com o mesmo reduzido, só insere.
        if ($qtdeComMesmoReduzido === 0) {
            $this->logger->info('Produto novo. Inserindo...');
            $estProduto = $this->saveProduto($ektProduto, null);
            $this->saveGrade($ektProduto, $estProduto);
            $this->insereNaReduzidoEktMesano($estProduto);
            $this->inseridos++;
        } else {
            $achouMesmo = false;
            // Começa a procurar o mesmo produto
            foreach ($produtosComMesmoReduzidoEKT as $mesmoReduzido) {

                $descricao_ekt = trim($ektProduto['DESCRICAO']);
                $nome = trim($mesmoReduzido['nome']);

                if ($descricao_ekt === $nome) {
                    // PRODUTO JÁ EXISTENTE
                    $achouMesmo = true;
                    $this->saveProduto($ektProduto, $mesmoReduzido);
                    $mesmoReduzido = $this->produto_model->findby_id($mesmoReduzido['id']); // recarrego para pegar oq foi alterado

                    $this->saveGrade($ektProduto, $mesmoReduzido);

                    // conferindo se as qtdes na grade batem
                    $qtdeTotal_ektProduto = $this->getQtdeTotalEktProduto($ektProduto);
                    $qtdeTotal_produto = $this->getQtdeTotalProduto($mesmoReduzido);
                    if ((float)$qtdeTotal_ektProduto !== (float)$qtdeTotal_produto) {
                        $this->logger->info('Qtde diferem para produtoId=[' . $mesmoReduzido['id'] . '] Reduzido:[' . $ektProduto['REDUZIDO'] . ']');
                        return;
                    }

                    $this->insereNaReduzidoEktMesano($mesmoReduzido);
                    $this->atualizados++;
                    break; // já achou, não precisa continuar procurando
                }
            }
            if (!$achouMesmo) {
                $this->logger->debug('Não achou o mesmo. Salvando um novo produto...');
                $produto = $this->saveProduto($ektProduto, null);
                $this->saveGrade($ektProduto, $produto);
                $this->insereNaReduzidoEktMesano($produto);
            }
        }
    }

    /**
     * Insere ou atualiza o est_produto e salvarProdutoPreco()
     *
     * @param $ektProduto
     * @param null $produto
     * @return null
     * @throws Exception
     */
    private function saveProduto($ektProduto, $produto = null)
    {
        $produtoNovo = $produto === null;

        $produto['subgrupo_id'] = $this->findSubgrupoByCodigo($ektProduto['SUBDEPTO']);
        if (!$produto['subgrupo_id']) {
            throw new RuntimeException('Subgrupo (antigo subdepto) não encontrado [' . $ektProduto['SUBDEPTO'] . ']');
        }

        $fornecedor_id = $this->fornecedor_model->findByCodigoEkt($ektProduto['FORNEC'], $this->mesano);
        if (!$fornecedor_id) {
            throw new RuntimeException('Fornecedor não encontrado: [' . $ektProduto['FORNEC'] . '] no mesano [' . $this->mesano . ']');
        }
        $produto['fornecedor_id'] = $fornecedor_id;

        $produto['nome'] = $ektProduto['DESCRICAO'];
        $produtoAtributos['5e46a239-98e4-421f-8bff-bc947d2330f4'] = $ektProduto['DATA_ULT_VENDA'];

        $produtoAtributos['5c0bf6e4-cc24-4e95-a05c-730ca79e8330'] = ImportarProdutos::$grades[$ektProduto['GRADE']]['uuid'];
        if (!$produtoAtributos['5c0bf6e4-cc24-4e95-a05c-730ca79e8330']) {
            throw new RuntimeException('Grade não encontrada [' . $ektProduto['GRADE'] . ']');
        }

        if ($produtoNovo) {
            $produtoAtributos['f1baa66b-c7ff-42f8-9554-d9fd3bf66123'] = $this->handleReduzido($ektProduto);
        }

        $produto['codigo_from'] = $ektProduto['REDUZIDO'];
        $produto['referencia'] = $ektProduto['REFERENCIA'];

        $produto['unidade_produto_id'] = $this->findUnidadeByLabel($ektProduto['UNIDADE']);

        if (!$produto['unidade_produto_id']) {
            throw new RuntimeException('Unidade não encontrada: [' . $ektProduto['UNIDADE'] . ']');
        }

        $produto['ncm'] = $ektProduto['NCM'] ? $ektProduto['NCM'] : '62179000';

        $produto['status'] = $this->importandoMesCorrente ? 'ATIVO' : 'INATIVO';
        $produto_id = $this->produto_model->save($produto);

        foreach ($produtoAtributos as $uuid => $valor) {
            $atributoId = $this->getAtributoByUUID($uuid);
            $sqlAtributo = 'SELECT * FROM est_produto_atributo WHERE atributo_id = ? AND produto_id = ?';
            $produtoAtributo = $this->dbcrosier->query($sqlAtributo, [$atributoId, $produto_id])->result_array();
            if (!isset($produtoAtributo[0])) {
                $produtoAtributo = [
                    'produto_id' => $produto_id,
                    'atributo_id' => $atributoId,
                    'soma_preench' => 'S',
                    'quantif' => 'N',
                    'precif' => 'N',
                ];
            } else {
                $produtoAtributo = $produtoAtributo[0];
            }
            $produtoAtributo['valor'] = $valor;
            $this->produtoatributo_model->save($produtoAtributo);
        }

        $produto['id'] = $produto_id;

        $this->salvarProdutoPreco($ektProduto, $produto_id, $this->mesano);

        return $produto;
    }


    /**
     * @param $codigo
     * @return mixed
     */
    private function findSubgrupoByCodigo($codigo)
    {
        if (!$this->subgrupos) {
            $this->subgrupos = [];
            $sql = 'SELECT id, codigo FROM est_subgrupo';
            $r = $this->dbcrosier->query($sql)->result_array();
            foreach ($r as $subdepto) {
                $this->subgrupos[$subdepto['codigo']] = $subdepto['id'];
            }
        }
        if (isset($this->subgrupos[$codigo])) {
            return $this->subgrupos[$codigo];
        } else {
            return $this->subgrupos['0'];
        }
    }


    /**
     * Monta o est_produto.reduzido
     *
     * @param $ektProduto
     * @return string|void
     */
    private function handleReduzido($ektProduto)
    {
        // O reduzido do Crosier sempre começa com o mesano
        $mesano_menor = substr($this->mesano, 2);
        $reduzido = $mesano_menor . str_pad($ektProduto['REDUZIDO'], 10, '0', STR_PAD_LEFT);

        while (true) {
            $existe = $this->dbcrosier->query('SELECT 1 FROM est_produto_atributo pa, est_atributo a WHERE pa.atributo_id = a.id AND a.uuid = \'f1baa66b-c7ff-42f8-9554-d9fd3bf66123\' AND pa.valor = ?', [
                $reduzido
            ])->result_array();
            if (count($existe) > 0) {
                $reduzido = $mesano_menor . '0' . str_pad(rand(1, 999), 3, '0', STR_PAD_LEFT) . '0' . str_pad($ektProduto['REDUZIDO'], 5, '0', STR_PAD_LEFT);
            } else {
                break;
            }
        }

        return $reduzido;

    }

    /**
     * @param $label
     * @return mixed
     */
    private function findUnidadeByLabel($label)
    {
        if (!$this->unidades) {
            $this->unidades = array();
            $sql = 'SELECT id, label FROM est_unidade_produto';
            $r = $this->dbcrosier->query($sql)->result_array();
            foreach ($r as $unidade) {
                $this->unidades[$unidade['label']] = $unidade['id'];
            }
        }
        if (!$label === null or strpos($label, 'PC') >= 0) {
            $label = 'UN';
        }
        // Se não achar, retorna o 999999 (ERRO DE IMPORTAÇÃO)
        return $this->unidades[$label] ? $this->unidades[$label] : $this->unidades['ERRO'];
    }

    /**
     * Salva o est_produto_preco.
     *
     * @param $produtoEkt
     * @param $produtoId
     * @param $mesano
     */
    private function salvarProdutoPreco($produtoEkt, $produtoId, $mesano): void
    {
        if (!$produtoEkt['DATA_PCUSTO']) {
            $produtoEkt['DATA_PCUSTO'] = DateTime::createFromFormat('Ymd', $mesano . '01')->format('Y-m-d');
        }

        if (!$produtoEkt['DATA_PVENDA']) {
            $produtoEkt['DATA_PVENDA'] = DateTime::createFromFormat('Ymd', $mesano . '01')->format('Y-m-d');
        }

        $query = $this->dbcrosier->get_where('est_produto_preco', [
            'produto_id' => $produtoId,
            'dt_custo' => $produtoEkt['DATA_PCUSTO'],
            'dt_preco_venda' => $produtoEkt['DATA_PVENDA'],
            'preco_custo' => $produtoEkt['PCUSTO'],
            'preco_vista' => $produtoEkt['PVISTA'],
            'preco_prazo' => $produtoEkt['PPRAZO']
        ]);
        $existe = $query->result_array();
        if (count($existe) > 0) {
            return;
        }

        $dtMesano = DateTime::createFromFormat('Ymd', $mesano . '01')->format('Y-m-d');

        $data = [
            'inserted' => date('Y-m-d H:i:s'),
            'updated' => date('Y-m-d H:i:s'),
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
        ];

        $this->dbcrosier->insert('est_produto_preco', $data) or $this->exit_db_error();

        $this->inseridos++;
    }

    /**
     * Salva os registros da est_produto_saldo.
     * @param $ektProduto
     * @param $produto
     * @throws Exception
     */
    private function saveGrade($ektProduto, $produto): void
    {
        $saldos = $this->dbcrosier->query('SELECT count(*) as qtde FROM est_produto_saldo WHERE produto_id = ?', [
            $produto['id']
        ])->result_array();

        if (count($saldos) > 0 && $saldos[0]['qtde'] > 0) {
            throw new RuntimeException('Já tem saldo [' . $produto['descricao'] . '] e não deveria por causa do truncate do começo.');
        }

        $qtdeTamanhos = ImportarProdutos::$grades[$ektProduto['GRADE']]['posicoes'];

        // Em alguns casos tem qtdes em gradestamanho além da capacidade da grade.
        // Aí acumulo tudo e salvo junto numa posição de grade que realmente exista (faça sentido).
        $acumulado = 0.0;
        $acumulado += ((float)$ektProduto['QT13'] !== 0.0) ? (float)$ektProduto['QT13'] : 0.0;
        $acumulado += ((float)$ektProduto['QT14'] !== 0.0) ? (float)$ektProduto['QT14'] : 0.0;
        $acumulado += ((float)$ektProduto['QT15'] !== 0.0) ? (float)$ektProduto['QT15'] : 0.0;
        if ($qtdeTamanhos < 12 && (float)$ektProduto['QT12'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT12'];
            $ektProduto['QT12'] = null;
        }
        if ($qtdeTamanhos < 11 && (float)$ektProduto['QT11'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT11'];
            $ektProduto['QT11'] = null;
        }
        if ($qtdeTamanhos < 10 && (float)$ektProduto['QT10'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT10'];
            $ektProduto['QT10'] = null;
        }
        if ($qtdeTamanhos < 9 && (float)$ektProduto['QT09'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT09'];
            $ektProduto['QT09'] = null;
        }
        if ($qtdeTamanhos < 8 && (float)$ektProduto['QT08'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT08'];
            $ektProduto['QT08'] = null;
        }
        if ($qtdeTamanhos < 7 && (float)$ektProduto['QT07'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT07'];
            $ektProduto['QT07'] = null;
        }
        if ($qtdeTamanhos < 6 && (float)$ektProduto['QT06'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT06'];
            $ektProduto['QT06'] = null;
        }
        if ($qtdeTamanhos < 5 && (float)$ektProduto['QT05'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT05'];
            $ektProduto['QT05'] = null;
        }
        if ($qtdeTamanhos < 4 && (float)$ektProduto['QT04'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT04'];
            $ektProduto['QT04'] = null;
        }
        if ($qtdeTamanhos < 3 && (float)$ektProduto['QT03'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT03'];
            $ektProduto['QT03'] = null;
        }
        if ($qtdeTamanhos < 2 && (float)$ektProduto['QT02'] !== 0.0) {
            $acumulado += (float)$ektProduto['QT02'];
            $ektProduto['QT02'] = null;
        }

        for ($i = 1; $i <= 12; $i++) {
            $qtde = (float)($ektProduto['QT' . str_pad($i, 2, '0', STR_PAD_LEFT)]);
            if ($qtde) {
                $this->saveProdutoSaldo($ektProduto, $produto, $i, $acumulado);
                $acumulado = 0.0; // já salvou, não precisa mais
            }
        }
        if ($acumulado) {
            $qtde = (float)($ektProduto['QT01']) ?: 0.0;
            $qtde += $acumulado;
            $this->saveProdutoSaldo($ektProduto, $produto, 1, $qtde);
        }
    }

    /**
     * Descobre a est_grade_tamanho e salva o est_produto_saldo.
     *
     * @param $ektProduto
     * @param $produto
     * @param $ordem
     * @param $acumulado
     * @throws Exception
     */
    private function saveProdutoSaldo($ektProduto, $produto, $ordem, $acumulado): void
    {
        $ordemStr = str_pad($ordem, 2, '0', STR_PAD_LEFT);

        $qtde = (float)$ektProduto['QT' . $ordemStr];
        $qtde += $acumulado;

        $qryGt = $this->dbcrosier->query('SELECT id FROM est_atributo WHERE atributo_pai_uuid = ? AND ordem = ?', [
            ImportarProdutos::$grades[$ektProduto['GRADE']]['uuid'],
            $ordem
        ])->result_array();

        if (count($qryGt) !== 1) {
            throw new RuntimeException('Erro ao pesquisar grade.');
        }
        $gt = $qryGt[0];

        $produtoSaldo['produto_id'] = $produto['id'];
        $produtoSaldo['qtde'] = $qtde;

        $produtoSaldoId = $this->produtosaldo_model->save($produtoSaldo) or $this->exit_db_error('Erro ao salvar na est_produto_saldo para o produto id [' . $produto['id'] . ']');

        $produtoSaldoAtributo['produto_saldo_id'] = $produtoSaldoId;
        $produtoSaldoAtributo['atributo_id'] = $gt['id'];

        $this->dbcrosier->query('INSERT INTO est_produto_saldo_atributo(produto_saldo_id, atributo_id) VALUES(?,?)', $produtoSaldoAtributo);
    }

    /**
     * Insere o registro na est_produto_reduzidoektmesano se ainda não existir para o $this->mesano.
     *
     * @param $produtoCrosier
     */
    private function insereNaReduzidoEktMesano($produtoCrosier): void
    {
        $produtoId = $produtoCrosier['id'];
        $reduzido_ekt = $produtoCrosier['codigo_from'];

        // Verifica se já tem registro marcando este produto no mesano
        $sql = 'SELECT * FROM est_produto_reduzidoektmesano WHERE produto_id = ? AND mesano = ? AND reduzido_ekt = ?';
        $params = array(
            $produtoId,
            $this->mesano,
            $reduzido_ekt
        );
        $r = $this->dbcrosier->query($sql, $params)->result_array();

        // Se ainda não tem na est_produto_reduzidoektmesano, insere...
        if (count($r) === 0) {
            $codektmesano['produto_id'] = $produtoId;
            $codektmesano['mesano'] = $this->mesano;
            $codektmesano['reduzido_ekt'] = $reduzido_ekt;
            $this->dbcrosier->insert('est_produto_reduzidoektmesano', $codektmesano) or $this->exit_db_error('Erro ao inserir na est_produto_reduzidoektmesano. produto id [' . $produtoId . ']');
        }
    }

    /**
     * @param $ektProduto
     * @return null|float
     */
    private function getQtdeTotalEktProduto($ektProduto): ?float
    {
        $qtdeTotal =
            ($ektProduto['QT01'] ?: 0.0) +
            ($ektProduto['QT02'] ?: 0.0) +
            ($ektProduto['QT03'] ?: 0.0) +
            ($ektProduto['QT04'] ?: 0.0) +
            ($ektProduto['QT05'] ?: 0.0) +
            ($ektProduto['QT06'] ?: 0.0) +
            ($ektProduto['QT07'] ?: 0.0) +
            ($ektProduto['QT08'] ?: 0.0) +
            ($ektProduto['QT09'] ?: 0.0) +
            ($ektProduto['QT10'] ?: 0.0) +
            ($ektProduto['QT11'] ?: 0.0) +
            ($ektProduto['QT12'] ?: 0.0) +
            ($ektProduto['QT13'] ?: 0.0) +
            ($ektProduto['QT14'] ?: 0.0) +
            ($ektProduto['QT15'] ?: 0.0);

        return (float)$qtdeTotal;
    }

    /**
     * @param $produto
     * @return float|null
     */
    private function getQtdeTotalProduto($produto): ?float
    {
        if (!$produto) {
            $this->logger->debug('PRODUTO === NULL ?????? ');
            return null;
        }
        $qtdeTotal = 0.0;

        $saldos = $this->dbcrosier->query('SELECT qtde FROM est_produto_saldo WHERE produto_id = ?', [
            $produto['id']
        ])->result_array();

        foreach ($saldos as $saldo) {
            if ($saldo['qtde']) {
                $qtdeTotal += (float)$saldo['qtde'];
            }
        }

        return (float)$qtdeTotal;
    }

    /**
     * Quando a importação é para o mês corrente, percorre os produtos que estavam com reduzido_ekt_ate = null e
     * verifica se não tiveram substituição de reduzido no EKT.
     */
    private function corrigeReduzidoEktAteMesCorrente(): void
    {
        $this->logger->info('.................... CORRIGINDO reduzido_ekt_ate PARA IMPORTAÇÃO DE MÊS CORRENTE...');
        // Pego todos os produtos que esteja com "reduzido_ekt_ate" = NULL mas que não estejam na lista dos produtos deste mês (ou seja, tiveram seu reduzido reutilizado neste mês por outro produto)
        $sql = 'SELECT produto_id FROM est_produto_atributo WHERE atributo_id = ? AND (valor IS NULL OR valor = \'\') AND produto_id NOT IN (SELECT produto_id FROM est_produto_reduzidoektmesano WHERE mesano = ?)';
        // ou seja, não existe um registro na est_produto_reduzidoektmesano para o mês corrente para tal est_produto.id
        $query = $this->dbcrosier->query($sql,
            [
                $this->getAtributoByUUID('06a32cc0-7cb0-4c58-9b4f-ca7776b7f8e3'), // reduzido_ekt_desde
                $this->agora->format('Ym'),
            ]);
        $result = $query->result_array();
        $total = count($result);
        $i = 1;

        $ultimoDiaMesAnterior = new DateTime();
        $ultimoDiaMesAnterior->setTime(0, 0, 0, 0);
        $ultimoDiaMesAnterior->setDate($ultimoDiaMesAnterior->format('Y'), $ultimoDiaMesAnterior->format('m') - 1, 15)->format('Y-m-t');
        $ultimoDiaMesAnterior = $ultimoDiaMesAnterior->format('Y-m-d');

        foreach ($result as $r) {
            try {
                $this->dbcrosier->update_string(
                    'est_produto_atributo',
                    ['valor' => $ultimoDiaMesAnterior],
                    [
                        'produto_id' => $r['produto_id'],
                        'atributo_id' => $this->atributos['06a32cc0-7cb0-4c58-9b4f-ca7776b7f8e3']
                    ]);

                $this->logger->info(' ... ' .
                    str_pad($i++, 6, '0', STR_PAD_LEFT) . '/' . str_pad($total, 6, '0', STR_PAD_LEFT) .
                    ' (' . $r['produto_id'] . ')');
            } catch (Exception $e) {
                print_r($e->getMessage());
                exit();
            }
        }

        $this->logger->info('.................... OK');

    }

    /**
     * @throws Exception
     */
    private function corrigirCampoAtual(): void
    {
        $mesanoAtual = (new DateTime())->format('Ym');
        $this->dbcrosier->query('UPDATE est_produto SET status = \'INATIVO\'');
        $this->dbcrosier->query('UPDATE est_produto SET status = \'ATIVO\' WHERE id IN (SELECT produto_id FROM est_produto_reduzidoektmesano WHERE mesano = ' . $mesanoAtual . ')');
    }

    /**
     * TRUNCATE na est_produto_reduzidoektmesano e NULL para reduzido_ekt_desde e reduzido_ekt_ate.
     * Percorre todos desde 201401 e vai inserindo na est_produto_reduzidoektmesano.
     */
    private function corrigirProdutosReduzidoEktDesdeAte(): void
    {
        $this->dbcrosier->trans_start();

        $this->logger->info('Iniciando correção para est_produto_reduzidoektmesano e NULL para reduzido_ekt_desde e reduzido_ekt_ate...');
        $this->logger->info('.');
        $this->logger->info('.');
        $this->logger->info('TRUNCATE TABLE est_produto_reduzidoektmesano');
        $this->logger->info('OK');

        $this->dbcrosier->query('TRUNCATE TABLE est_produto_reduzidoektmesano');
        $mesano_ini = '201401';
        $hoje = new DateTime();
        $mesano_fim = $hoje->format('Ym');
        $mesesanos = Datetime_utils::mesano_list($mesano_ini, $mesano_fim);

        foreach ($mesesanos as $mesano) {
            $this->mesano = $mesano;
            $this->dtMesano = DateTime::createFromFormat('Ymd', $mesano . '01');
            $this->dtMesano->setTime(0, 0, 0, 0);

            // Pega todos os produtos da ekt_produto para o $mesano
            $query = $this->dbekt->get_where('ekt_produto', ['mesano' => $mesano]);//, 'reduzido' => 1507]);
            $result = $query->result_array();
            $total = count($result);

            $i = 1;
            foreach ($result as $r) {
                try {

                    // Para cada ekt_produto, encontra o est_produto
                    $query = $this->dbcrosier->query('SELECT * FROM est_produto WHERE codigo_from = ? AND trim(nome) LIKE ?', [
                        $r['REDUZIDO'],
                        trim($r['DESCRICAO'])
                    ]);
                    $result = $query->result_array();
                    $qtde = count($result);
                    if ($qtde !== 1) {
                        throw new RuntimeException("Erro. Qtde deveria ser exatamente 1 para reduzido = '" . $r['REDUZIDO'] . "' e descricao = '" . $r['DESCRICAO'] . "'. QTDE='" . $qtde . "'");
                    }
                    $this->insereNaReduzidoEktMesano($result[0]);
                    $this->logger->info($mesano . ' ... ' . str_pad($r['REDUZIDO'], 6, 0, STR_PAD_LEFT) . ' ....................................................................... ' . str_pad($i++, 6, '0', STR_PAD_LEFT) . '/' . str_pad($total, 6, '0', STR_PAD_LEFT) . ' (' . $r['id'] . ')');
                } catch (Exception $e) {
                    print_r($e->getMessage());
                    exit();
                }
            }
        }

        $this->logger->info('Finalizando... commitando a transação...');
        $this->dbcrosier->trans_complete();
    }

    /**
     * Corrige os campos reduzido_ekt_desde e reduzido_ekt_ate (de toda a est_produto ou somente dos registros que são do mesano passado).
     *
     * @throws Exception
     */
    private function corrigeReduzidoEktDesdeAte(): void
    {
        $this->logger->info('Iniciando correção de reduzido_ekt_desde e reduzido_ekt_ate');

        $this->dbcrosier->query('DELETE FROM est_produto_atributo WHERE atributo_id = \'fa0e7196-4b7f-4797-97ab-70615a72fcf0\'');
        $this->dbcrosier->query('DELETE FROM est_produto_atributo WHERE atributo_id = \'06a32cc0-7cb0-4c58-9b4f-ca7776b7f8e3\'');
        $this->logger->info('OK');

        $sql = "SELECT * FROM est_produto WHERE codigo_from != 88888 AND TRIM(nome) != ''";
        $rs = $this->dbcrosier->query($sql)->result_array();

        // Monto toda a tabela num array para não precisar executar um SELECT pra cada produto no foreach.
        $sql = 'SELECT * FROM est_produto_reduzidoektmesano ORDER BY produto_id, mesano';
        $todos = $this->dbcrosier->query($sql)->result_array();
        if (!$todos or count($todos) < 1) {
            throw new Exception('Nenhum registro encontrado na est_produto_reduzidoektmesano');
        }
        $reduzidosektmesano = [];
        foreach ($todos as $t) {
            if (!array_key_exists($t['produto_id'], $reduzidosektmesano)) {
                $reduzidosektmesano[$t['produto_id']] = [];
            }
            array_push($reduzidosektmesano[$t['produto_id']], $t['mesano']);
        }

        $i = 1;
        $total = count($rs);
        foreach ($rs as $estProduto) {
            $mesesanos = $reduzidosektmesano[$estProduto['id']];

            $mesano_ini = $mesesanos[0];
            $mesano_fim = $mesesanos[count($mesesanos) - 1];

            $atributoId = $this->getAtributoByUUID('fa0e7196-4b7f-4797-97ab-70615a72fcf0'); // reduzido_ekt_desde
            $reduzidoEktDesde = [
                'produto_id' => $estProduto['id'],
                'atributo_id' => $atributoId,
                'soma_preench' => 'S',
                'quantif' => 'N',
                'precif' => 'N',
                'valor' => (DateTime::createFromFormat('Ym', $mesano_ini))->format('Y-m-d')
            ];
            $this->produtoatributo_model->save($reduzidoEktDesde);

            $atributoId = $this->getAtributoByUUID('06a32cc0-7cb0-4c58-9b4f-ca7776b7f8e3'); // reduzido_ekt_ate
            $reduzidoEktAte = [
                'produto_id' => $estProduto['id'],
                'atributo_id' => $atributoId,
                'soma_preench' => 'S',
                'quantif' => 'N',
                'precif' => 'N',
                'valor' => (DateTime::createFromFormat('Ym', $mesano_fim))->format('Y-m-t')
            ];
            $this->produtoatributo_model->save($reduzidoEktAte);

            $this->logger->info(' ... ' . str_pad($estProduto['reduzido_ekt'], 6, 0, STR_PAD_LEFT) . ' ....................................................................... ' . str_pad($i++, 6, '0', STR_PAD_LEFT) . '/' . str_pad($total, 6, '0', STR_PAD_LEFT));

        }
        $mesano_atual = $this->agora->format('Ym');
        $this->dbcrosier->query("UPDATE est_produto_atributo SET valor = NULL WHERE uuid = '06a32cc0-7cb0-4c58-9b4f-ca7776b7f8e3' AND DATE_FORMAT(valor, '%Y%m') = ?", [$mesano_atual]);
        $this->logger->info('OK');
    }

    /**
     * TRUNCATE TABLE est_produto_preco e reinserções desde 201401.
     */
    private function corrigirPrecos(): void
    {
        $this->dbcrosier->trans_start();

        $this->dbcrosier->query('TRUNCATE TABLE est_produto_preco');

        $mesano_ini = '201401';
        $hoje = new DateTime();
        $mesano_fim = $hoje->format('Ym');

        $mesesanos = Datetime_utils::mesano_list($mesano_ini, $mesano_fim);

        foreach ($mesesanos as $mesano) {
            $this->mesano = $mesano;

            $query = $this->dbekt->get_where('ekt_produto', array(
                'mesano' => $mesano
            ));
            $result = $query->result_array();

            // Pega todos os produtos da ekt_produto para o $mesano
            $i = 1;
            foreach ($result as $r) {
                try {
                    // Para cada ekt_produto, encontra o est_produto
                    $query = $this->dbcrosier->query('SELECT * FROM est_produto WHERE codigo_from = ? AND trim(nome) LIKE ? ', [
                        $r['REDUZIDO'],
                        trim($r['DESCRICAO'])
                    ]);
                    $result = $query->result_array();
                    $qtde = count($result);
                    if ($qtde !== 1) {
                        throw new RuntimeException("Erro. Qtde deveria ser exatamente 1 para reduzido = '" . $r['REDUZIDO'] . "' e descricao = '" . $r['DESCRICAO'] . "'. QTDE='" . $qtde . "'");
                    }
                    $produto = $result[0];
                    $this->salvarProdutoPreco($r, $produto['id'], $mesano);
                    $this->logger->info($i++ . ' (' . $r['id'] . ')');
                } catch (Exception $e) {
                    print_r($e->getMessage());
                    exit();
                }
            }
        }

        $this->dbcrosier->trans_complete();

        $this->logger->info(PHP_EOL);
        $this->logger->info('--------------------------------------------------------------');
        $this->logger->info('--------------------------------------------------------------');
        $this->logger->info('--------------------------------------------------------------');
    }

    /**
     * @param string $uuid
     * @return mixed
     */
    public function getAtributoByUUID(string $uuid)
    {
        if (!isset($this->atributos[$uuid])) {
            $r = $this->dbcrosier->query('SELECT * FROM est_atributo WHERE uuid = ?', [$uuid])->result_array() or die('Atributo não encontrado para uuid = "' . $uuid . '"');
            $this->atributos[$uuid] = $r[0]['id'];
        }
        return $this->atributos[$uuid];
    }


    public static $grades = [
        1 => [
            'uuid' => '750d27dd-7e1f-4fa2-91de-3433bafd4ac6', 'posicoes' => 7
        ],
        2 => [
            'uuid' => 'f55b404d-0344-494b-bf88-95eb92ae5c14', 'posicoes' => 10
        ],
        3 => [
            'uuid' => 'dfdd2b4a-040d-4748-9eaa-ed0f76f5aa65', 'posicoes' => 12
        ],
        4 => [
            'uuid' => '1eebd976-48e8-4529-a711-395f932fa52a', 'posicoes' => 12
        ],
        5 => [
            'uuid' => '5bfa9908-1c2e-4b36-b69c-8ecfab5e1bdd', 'posicoes' => 12
        ],
        6 => [
            'uuid' => '54faa8f3-b0a9-4127-a4e8-0aa79749dff2', 'posicoes' => 12
        ],
        7 => [
            'uuid' => 'd902289b-3c3f-4284-ae74-273a4be0b4ce', 'posicoes' => 12
        ],
        8 => [
            'uuid' => '150f1ef0-e0a3-4ddf-9fce-e81427f24935', 'posicoes' => 12
        ],
        9 => [
            'uuid' => 'ad2b24e5-c012-4a73-a693-0041836cd877', 'posicoes' => 11
        ],
        10 => [
            'uuid' => '7b039a20-390f-425b-b72a-bc2d30bcbbc5', 'posicoes' => 1
        ],
        11 => [
            'uuid' => '186bff19-0b26-47a6-9e77-81539277f230', 'posicoes' => 1
        ],
        12 => [
            'uuid' => '946a4a1e-b392-4bff-9065-4bd48e78b8bd', 'posicoes' => 1
        ],
        13 => [
            'uuid' => '8726ac02-1489-4a2d-b215-316c5ee307b0', 'posicoes' => 8
        ],
        14 => [
            'uuid' => '8637de33-1e60-4773-a864-66353411d8a1', 'posicoes' => 10
        ],
        15 => [
            'uuid' => '1aaa4ce9-87eb-41f8-a6cd-4abc383f49bb', 'posicoes' => 7
        ],
        16 => [
            'uuid' => '512df5c9-6ac0-4fc1-a50c-073c9605bc96', 'posicoes' => 12
        ],

    ];


}





