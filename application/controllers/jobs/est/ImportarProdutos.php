<?php


use CIBases\Models\DAO\Base\Base_model;

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
	/** @var Base_model */
	public $produtosaldo_model;

	/** @var Produto_model */
	public $produto_model;

	/** @var Base_model */
	public $preco_model;

	/** @var Fornecedor_model */
	public $fornecedor_model;

	/** @var Ektproduto_model */
	public $ektproduto_model;

	/** @var LogWriter */
	private $logger;

	/** @var DateTime */
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

	/** @var int */
	private $inseridos;

	/** @var int */
	private $atualizados;

	/**  @var CI_DB_mysqli_driver */
	private $dbekt;

	/** @var CI_DB_mysqli_driver */
	private $dbcrosier;

	/**
	 * Os grupos no crosier são os deptos do ekt
	 * @var array
	 */
	private $grupos;

	/**
	 * Os subgrupos no crosier são os subdeptos/modelos do ekt
	 * @var array
	 */
	private $subgrupos;

	/** @var array */
	private $unidades;

	private static $grades;

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

		$this->preco_model = new Base_model('est_produto_preco', 'crosier');
		$this->preco_model->setDb($this->dbcrosier);

		$this->load->model('est/fornecedor_model');
		$this->fornecedor_model->setDb($this->dbcrosier);

		$this->load->model('ekt/ektproduto_model');
		$this->ektproduto_model->setDb($this->dbekt);

		$this->produtosaldo_model = new Base_model('est_produto_saldo', 'crosier');
		$this->produtosaldo_model->setDb($this->dbcrosier);

		self::$grades = json_decode($this->dbcrosier->query('SELECT * FROM cfg_app_config WHERE chave = \'est_produto_grades\'')->result_array()[0]['valor'], true);

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
			$this->corrigirCampoAtual();
		}
		if ($acao === 'PRECOS') {
			$this->corrigirPrecos();
		}

		$time_end = microtime(true);
		$execution_time = ($time_end - $time_start);
		$this->logger->info(PHP_EOL);
		$this->logger->info('----------------------------------');
		$this->logger->info('Tempo total: ' . $execution_time . 's');
		// $this->logger->sendMail();
		$this->logger->closeLog();
	}

	/**
	 */
	private function deletarSaldos(): void
	{
		$this->dbcrosier->query('DELETE FROM est_produto_reduzidoektmesano WHERE mesano = ?', [$this->mesano]);
		$this->dbcrosier->query('SET FOREIGN_KEY_CHECKS = 0') or $this->exit_db_error('Erro ao SET FOREIGN_KEY_CHECKS = 0');
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
		//$l = $this->dbekt->query("SELECT * FROM ekt_produto WHERE reduzido = 5750 AND mesano = ?", array($this->mesano))->result_array();

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
		$produto['depto_id'] = 1;
		$jsonData['depto_codigo'] = 1;
		$jsonData['depto_nome'] = 'LOJA';

		$grupo = $this->findGrupoByCodigo($ektProduto['DEPTO']);
		if (!$grupo) {
			throw new RuntimeException('Grupo (antigo depto) não encontrado [' . $ektProduto['DEPTO'] . ']');
		}
		$produto['grupo_id'] = $grupo['id'];
		$jsonData['grupo_codigo'] = $grupo['codigo'];
		$jsonData['grupo_nome'] = $grupo['nome'];

		$subgrupo = $this->findSubgrupoByCodigo($ektProduto['SUBDEPTO']);
		if (!$subgrupo) {
			throw new RuntimeException('Subgrupo (antigo subdepto/modelo) não encontrado [' . $ektProduto['SUBDEPTO'] . ']');
		}
		$produto['subgrupo_id'] = $subgrupo['id'];
		$jsonData['subgrupo_codigo'] = $subgrupo['codigo'];
		$jsonData['subgrupo_nome'] = $subgrupo['nome'];

		$fornecedor_id = $this->fornecedor_model->findByCodigoEkt($ektProduto['FORNEC'], $this->mesano);
		if (!$fornecedor_id) {
			throw new RuntimeException('Fornecedor não encontrado: [' . $ektProduto['FORNEC'] . '] no mesano [' . $this->mesano . ']');
		}
		$produto['fornecedor_id'] = $fornecedor_id;

		$produto['nome'] = $ektProduto['DESCRICAO'];

		$jsonData['dt_ult_venda'] = $ektProduto['DATA_ULT_VENDA'];
		$jsonData['grade'] = trim($ektProduto['GRADE']);

		$jsonData['reduzido'] = $this->handleReduzido($ektProduto);

		$jsonData['reduzido'] = trim($ektProduto['REDUZIDO']);
		$jsonData['referencia'] = trim($ektProduto['REFERENCIA']);

		$jsonData['unidade_produto'] = trim($ektProduto['UNIDADE']);
		$unidades = ['PC','PAR','MT','CJ','JG','PCT','KIT','UN','CX'];
		if (!in_array($jsonData['unidade_produto'], $unidades)) {
			$jsonData['unidade_produto'] = 'UN';
		}

		$jsonData['ncm'] = $ektProduto['NCM'] ? trim($ektProduto['NCM']) : '62179000';

		$produto['status'] = $this->importandoMesCorrente ? 'ATIVO' : 'INATIVO';

		$produto['json_data'] = json_encode($jsonData);

		$produto_id = $this->produto_model->save($produto);

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
			$sql = 'SELECT id, codigo, nome FROM est_subgrupo';
			$r = $this->dbcrosier->query($sql)->result_array();
			foreach ($r as $subdepto) {
				$this->subgrupos[$subdepto['codigo']] = $subdepto;
			}
		}
		if (isset($this->subgrupos[$codigo])) {
			return $this->subgrupos[$codigo];
		} else {
			return $this->subgrupos['0'];
		}
	}

	/**
	 * O est_produto.grupo é o ekt DEPTO
	 *
	 * @param $codigo
	 * @return mixed
	 */
	private function findGrupoByCodigo($codigo)
	{
		if (!$this->grupos) {
			$this->grupos = [];
			$sql = 'SELECT id, codigo, nome FROM est_grupo';
			$r = $this->dbcrosier->query($sql)->result_array();
			foreach ($r as $grupo) {
				$this->grupos[$grupo['codigo']] = $grupo;
			}
		}
		if (isset($this->grupos[$codigo])) {
			return $this->grupos[$codigo];
		} else {
			return $this->grupos['0'];
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
			$existe = $this->dbcrosier->query('SELECT 1 FROM est_produto WHERE json_data->>"$.reduzido" = ?', [$reduzido])->result_array();
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



		$qtdeTamanhos = count(ImportarProdutos::$grades[$ektProduto['GRADE']]['tamanhos']);

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

		for ($i = 1; $i <= $qtdeTamanhos; $i++) {
			$qtde = (float)$ektProduto['QT' . str_pad($i, 2, '0', STR_PAD_LEFT)];

			$selec = $ektProduto['F' . $i] === 'S';
			if ($selec) {
				$this->saveProdutoSaldo($produto['id'], $ektProduto['GRADE'], $i, $qtde);
				$acumulado = 0.0; // já salvou, não precisa mais
			}
		}
		if ($acumulado) {
			$qtde = (float)$ektProduto['QT01'] ?: 0.0;
			$qtde += $acumulado;
			$this->saveProdutoSaldo($produto['id'], $ektProduto['GRADE'], 1, $qtde);
		}
	}

	/**
	 * Descobre a est_grade_tamanho e salva o est_produto_saldo.
	 * @param $produtoId
	 * @param $grade
	 * @param $ordem
	 * @param $qtde
	 * @throws Exception
	 */
	private function saveProdutoSaldo($produtoId, $grade, $ordem, $qtde): void
	{
		$produtoSaldo['produto_id'] = $produtoId;
		$produtoSaldo['qtde'] = $qtde;
		$jsonData['grade'] = $grade;
		$jsonData['ordem'] = $ordem;
		$produtoSaldo['json_data'] = json_encode($jsonData);
		$produtoSaldoId = $this->produtosaldo_model->save($produtoSaldo) or $this->exit_db_error('Erro ao salvar na est_produto_saldo para o produto id [' . $produtoId . ']');
	}

	/**
	 * Insere o registro na est_produto_reduzidoektmesano se ainda não existir para o $this->mesano.
	 *
	 * @param $produtoCrosier
	 */
	private function insereNaReduzidoEktMesano($produtoCrosier): void
	{
		$produtoId = $produtoCrosier['id'];
		$reduzido_ekt = json_decode($produtoCrosier['json_data'], true)['reduzido'];

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
	 * @throws Exception
	 */
	private function corrigirCampoAtual(): void
	{
		$mesanoAtual = (new DateTime())->format('Ym');
		$this->dbcrosier->query('UPDATE est_produto SET status = \'INATIVO\'');
		$this->dbcrosier->query('UPDATE est_produto SET status = \'ATIVO\' WHERE id IN (SELECT produto_id FROM est_produto_reduzidoektmesano WHERE mesano = ' . $mesanoAtual . ')');
	}





}





