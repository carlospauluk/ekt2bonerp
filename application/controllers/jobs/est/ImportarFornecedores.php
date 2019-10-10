<?php

require_once('./application/libraries/file/LogWriter.php');

/**
 * Job que realiza a importação dos fornecedores a partir da tabela ekt_fornecedor.
 *
 *
 *
 */
class ImportarFornecedores extends CI_Controller
{

    /**
     * @var LogWriter
     */
    private $logger;

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

    private $inseridos = 0;

    private $atualizados = 0;

    private $acertados_depara = 0;

    /**
     * @var CI_DB_mysqli_driver
     */
    private $dbekt;

    /**
     * @var CI_DB_mysqli_driver
     */
    private $dbcrosier;

    /**
     * @var Fornecedor_model
     */
    public $fornecedor_model;

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');

        $this->dbekt = $this->load->database('ekt', TRUE);
        $this->dbcrosier = $this->load->database('crosier', TRUE);

        error_reporting(E_ALL);
        ini_set('display_errors', 1);
    }

    /**
     * Importa os fornecedores da ekt_fornecedor
     * @param string|null $mesano
     * @throws Exception
     */
    public function importar(?string $mesano = null)
    {
        $time_start = microtime(true);
        $this->dbcrosier->trans_start();

        $this->load->model('est/fornecedor_model');
        $this->fornecedor_model->setDb($this->dbcrosier);

        $this->mesano = $mesano ?? (new DateTime())->format('Ym');
        $this->dtMesano = DateTime::createFromFormat('Ymd', $this->mesano . "01");
        $this->dtMesano->setTime(0, 0, 0, 0);
        if (!$this->dtMesano instanceof DateTime) {
            die("mesano inválido.\n\n\n");
        }

        $logPath = getenv('EKT_LOG_PATH') ?: './log/';

        $prefix = "ImportarFornecedores" . '_' . $mesano . '_';

        $this->logger = new LogWriter($logPath, $prefix);


        $sql = "SELECT * FROM ekt_fornecedor WHERE mesano = ? ORDER BY id";
        $query = $this->dbekt->query($sql, $this->mesano) or $this->exit_db_error();
        $result = $query->result_array();

        // Para cada um dos ekt_fornecedor...
        foreach ($result as $fornecedorEkt) {

            $codigoEkt = $fornecedorEkt['CODIGO'];
            $nomeFantasia = trim($fornecedorEkt['NOME_FANTASIA']);

            if ($nomeFantasia == '') {
                $this->logger->info(" >>>>>>>>>>>>>>>>>>>> FORNECEDOR com código = '" . $codigoEkt . " está sem nome fantasia. PULANDO.");
                continue;
            }

            $this->logger->info("EKT >>>>> Código: [" . $codigoEkt . "] Nome Fantasia: [" . $nomeFantasia . "]");

            // Pesquisa nos est_fornecedor pelo mesmo codigo_ekt
            $params = [
                $codigoEkt,
                $this->mesano
            ];
            $query = $this->dbcrosier->query('SELECT * FROM est_fornecedor_codektmesano WHERE codigo_ekt = ? AND mesano = ?', $params) or $this->exit_db_error();
            $r = $query->result_array();

            if (count($r) > 1) {
                throw new Exception("Mais de um fornecedor encontrado com o mesmo código (" . $codigoEkt . ")");
            } else {
                // Não encontrou
                if (count($r) == 0) {
                    // Simplesmente salva
                    $this->logger->info("Não existente. Salvando...");
                    $this->salvarFornecedor($fornecedorEkt);
                    $this->inseridos++;
                } else {
                    $this->logger->info("Já existente. Atualizando...");
                    $this->salvarFornecedor($fornecedorEkt, $r[0]['fornecedor_id']);
                    $this->atualizados++;
                }
                $this->logger->debug("OK!!!");
            }
        }

        $this->dbcrosier->trans_complete();

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
        $this->logger->info("Total Execution Time: " . $execution_time . "s");
        $this->logger->info("----------------------------------");

        $this->logger->sendMail();
        $this->logger->closeLog();
    }

    /**
     *
     * @param
     *            $fornecedorCrosier
     * @param
     *            $fornecedorEkt
     * @throws Exception
     */
    private function salvarFornecedor($fornecedorEkt, $fornecedorCrosier_id = null)
    {
        // $fornecedorCrosier = $this-> array(); // limpo o array pq ele na verdade veio da vw_est_fornecedor, portanto com campos a mais
        $fornecedorCrosier = $this->fornecedor_model->findby_id($fornecedorCrosier_id);

        if ($fornecedorCrosier_id) {
            $atualizando = true;
            $this->logger->debug("ATUALIZANDO fornecedor... ");
        } else {
            $this->logger->debug("INSERINDO novo fornecedor... ");
            $fornecedorCrosier['codigo'] = $this->findNovoCodigo($fornecedorEkt['CODIGO']);
        }
        $this->logger->debug($fornecedorEkt['CODIGO'] . " - " . $fornecedorEkt['NOME_FANTASIA']);

        $fornecedorCrosier['inscricao_estadual'] = trim($fornecedorEkt['INSC']);

        $cnpj = preg_replace('/[^\d]/', '', $fornecedorEkt['CGC']); // remove tudo o que não for número
        $cnpj = sprintf("%014d", $cnpj); // preenche com zeros, caso não tenha 14 dígitos
        $nomeFantasia = $fornecedorEkt['NOME_FANTASIA'] ? $fornecedorEkt['NOME_FANTASIA'] : "";
        $razaoSocial = $fornecedorEkt['RAZAO'] ? $fornecedorEkt['RAZAO'] : $nomeFantasia;

        $fornecedorCrosier['documento'] = trim($cnpj);
        $fornecedorCrosier['nome'] = trim($razaoSocial);
        $fornecedorCrosier['nome_fantasia'] = trim($nomeFantasia);

        $fornecedorId = $this->fornecedor_model->save($fornecedorCrosier) or $this->exit_db_error();
        $fornecedorCrosier['id'] = $fornecedorId;
        $this->salvarNaDePara($fornecedorCrosier, $fornecedorEkt);
    }

    /**
     *
     * @param
     *            $fornecedorCrosier
     * @param
     *            $fornecedorEkt
     */
    private function salvarNaDePara($fornecedorCrosier, $fornecedorEkt)
    {
        $fornecedorId = $fornecedorCrosier['id'];
        $this->logger->debug("LIDANDO COM 'depara' [$fornecedorId]...");

        $sql = "SELECT * FROM est_fornecedor_codektmesano WHERE fornecedor_id = ? AND mesano = ? AND codigo_ekt = ?";
        $params = array(
            $fornecedorId,
            $this->mesano,
            $fornecedorEkt['CODIGO']
        );
        $r = $this->dbcrosier->query($sql, $params)->result_array();

        // Se ainda não tem na est_fornecedor_codektmesano, insere...
        if (count($r) == 0) {
            $codektmesano['fornecedor_id'] = $fornecedorId;
            $codektmesano['mesano'] = $this->mesano;
            $codektmesano['codigo_ekt'] = $fornecedorEkt['CODIGO'];

            $this->dbcrosier->insert('est_fornecedor_codektmesano', $codektmesano) or $this->exit_db_error("Erro ao inserir na depara. fornecedor id [" . $fornecedorId . "]");
        }

    }

    /**
     * @param $codigoEkt
     * @return string|null
     */
    private function findNovoCodigo($codigoEkt)
    {
        $query = $this->dbcrosier->get_where('est_fornecedor', array(
            'codigo' => $codigoEkt
        )) or $this->exit_db_error();
        if (count($query->result_array()) == 0) {
            return $codigoEkt;
        }

        $codigoStr = null;
        // codigo tem 9 dígitos (999999001)
        while (true) {
            $prefix = rand(1, 99999);
            $codigoStr = str_pad($prefix, 5, "9", STR_PAD_LEFT) . "0" . $codigoEkt;
            $query = $this->dbcrosier->get_where('est_fornecedor', [
                'codigo' => $codigoStr
            ]) or $this->exit_db_error("Erro ao buscar código randômico.");
            $existe = $query->result_array();
            if (count($existe) == 0) {
                return $codigoStr;
            }
        }
    }


    private function exit_db_error($msg = null)
    {
        $this->logger->info(str_pad("", 100, "*"));
        $this->logger->info($msg ? $msg : '');
        $this->logger->info("LAST QUERY: " . $this->dbcrosier->last_query());
        print_r($this->dbcrosier->error());
        $this->logger->info(str_pad("", 100, "*"));
        exit();
    }
}
