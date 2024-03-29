<?php
require_once('./application/libraries/file/LogWriter.php');

/**
 * Job que realiza a importação dos dados nos CSVs gerados pelo EKT para as tabelas espelho (ekt_*).
 *
 * Para rodar:
 *
 * Pela linha de comando, chamar com:
 *
 *
 * set EKT_CSVS_PATH=\\10.1.1.100\export
 * set EKT_LOG_PATH=C:\ekt2bonerp\log
 *
 * export EKT_CSVS_PATH=/mnt/10.1.1.100-export
 * export EKT_LOG_PATH=~/dev/github/ekt2bonerp/log
 *
 * php index.php jobs/ekt/ImportarEkt2Espelhos importar YYYYMM FOR-PROD-...
 *
 * export XDEBUG_CONFIG="idekey=session_name"
 */
class ImportarEkt2Espelhos extends CI_Controller
{

    private $logger;

    private $agora;

    /**
     * Qual a pasta dos CSVs.
     * Será obtiecdo a partir da variável de ambiente EKT_CSVS_PATH.
     *
     * @var string
     */
    private $csvsPath;

    /**
     * Passado pela linha de comando no formato YYYYMM.
     *
     * @var string
     */
    private $mesAno;

    /**
     * $mesAno convertido para DateTime.
     *
     * @var DateTime
     */
    private $dtMesAno;

    /**
     * Conexão ao db ekt.
     */
    private $dbekt;

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');

        $this->dbekt = $this->load->database('ekt', TRUE);

        $this->load->library('datetime_library');
        error_reporting(E_ALL);
        ini_set('display_errors', 1);

        $this->load->model('est/produto_model');
        $this->agora = new DateTime();
    }

    /**
     * Método principal.
     *
     * $mesano (deve ser passado no formato YYYYMM).
     * $importadores (GERAIS,FOR,PROD,PED,VEN,ENC).
     */
    public function importar($mesano, $importadores)
    {
        $time_start = microtime(true);

        $this->csvsPath = getenv('EKT_CSVS_PATH') or die("EKT_CSVS_PATH não informado\n\n\n");
        $logPath = getenv('EKT_LOG_PATH') or die("EKT_LOG_PATH não informado\n\n\n");

        $this->mesano = $mesano;

        $this->csvsPath .= "/" . $mesano . "/";

        $this->dtMesAno = DateTime::createFromFormat('Ymd', $mesano . "01");
        if (!$this->dtMesAno instanceof DateTime) {
            die("mesano inválido.\n\n\n");
        }

        $prefix = is_array($importadores) ? implode('-', $importadores) : $importadores;
        $prefix = "ImportarEkt2Espelhos" . '_' . $mesano . '_' . $prefix . "_";

        $this->logger = new LogWriter($logPath, $prefix);

        $this->logger->info("Iniciando a importação para o mês/ano: [" . $mesano . "]");

        $this->logger->info("csvsPath: [" . $this->csvsPath . "]");
        $this->logger->info("logPath: [" . $logPath . "]");

        $tiposImportacoes = explode("-", $importadores);

        foreach ($tiposImportacoes as $tipo) {
            if (!in_array($tipo, array(
                'GERAIS',
                'FOR',
                'PROD',
                'PED',
                'VEN',
                'ENC'
            ))) {
                $this->logger->info("Tipo de importação inválido: [" . $tipo . "]");
            }
        }

        if (in_array('GERAIS', $tiposImportacoes)) {
            $this->importarDeptos();
            $this->importarSubdeptos();
        }
        if (in_array('FOR', $tiposImportacoes)) {
            $this->importarFornecedores();
        }
        if (in_array('PROD', $tiposImportacoes)) {
            $this->importarProdutos();
        }
        if (in_array('VEN', $tiposImportacoes)) {
            $this->importarVendedores();
            $this->importarVendas();
            $this->importarVendasItens();
        }
        if (in_array('PED', $tiposImportacoes)) {
            $this->importarPedidos();
        }
        if (in_array('ENC', $tiposImportacoes)) {
            $this->importarEncomendas();
            $this->importarEncomendasItens();
        }

        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        $this->logger->info(PHP_EOL . PHP_EOL . "---------------------------------- Total Execution Time: " . $execution_time . "s");
        // $this->logger->sendMail();
        $this->logger->closeLog();
    }

    /*
     *
     */
    private function importarDeptos()
    {
        $this->dbekt->trans_start();

        $this->logger->info("IMPORTANDO DEPTOS...");

        if (!$this->dbekt->query("DELETE FROM ekt_depto WHERE mesano = ?", array(
            $this->mesano
        ))) {
            $this->logger->debug('DELETE FROM ekt_depto WHERE mesano = ?');
            return;
        }

        $linhas = file($this->csvsPath . "est_d003.csv");

        $this->logger->info(count($linhas) . " depto(s) encontrado(s).");

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", $linha);
            if (count($campos) < 6) {
                $this->logger->info("A linha deve ter 6 campos. LINHA: [" + $linha + "]");
                return;
            }

            $ektDepto = array();

            // RECORD_NUMBER 4 INTEGER
            // CODIGO 3 DECIMAL
            // DESCRICAO 12 VARCHAR
            // MARGEM 3 DECIMAL
            // PECAS_AC 3 DECIMAL
            // VENDAS_AC 3 DECIMAL

            $ektDepto['RECORD_NUMBER'] = $i;
            $ektDepto['CODIGO'] = $campos[1];
            $ektDepto['DESCRICAO'] = $campos[2];
            $ektDepto['MARGEM'] = $campos[3];
            $ektDepto['PECAS_AC'] = $campos[4];
            $ektDepto['VENDAS_AC'] = $campos[5];
            $ektDepto['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektDepto);

            if (!$this->dbekt->insert('ekt_depto', $ektDepto)) {
                $this->logger->info('Erro ao salvar o ektDepto');
                return;
            } else {
                $this->logger->debug($i . " depto(s) inserido(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /*
     *
     */
    private function importarSubdeptos()
    {
        $this->dbekt->trans_start();

        $this->logger->info("IMPORTANDO SUBDEPTOS...");

        if (!$this->dbekt->query("DELETE FROM ekt_subdepto WHERE mesano = ?", array(
            $this->mesano
        ))) {
            $this->logger->info('ERRO em DELETE FROM ekt_subdepto WHERE mesano = ?');
            return;
        }

        $linhas = file($this->csvsPath . "est_d004.csv");

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", $linha);
            if (count($campos) < 29) {
                $this->logger->debug("A linha deve ter 29 campos. LINHA: [" + $linha + "]");
                exit;
            }

            $ektSubdepto = array();

            // RECORD_NUMBER 4 INTEGER
            // CODIGO 3 DECIMAL
            // DESCRICAO 12 VARCHAR
            // MARGEM 3 DECIMAL
            // PECAS_AC01 3 DECIMAL
            // PECAS_AC02 3 DECIMAL
            // PECAS_AC03 3 DECIMAL
            // PECAS_AC04 3 DECIMAL
            // PECAS_AC05 3 DECIMAL
            // PECAS_AC06 3 DECIMAL
            // PECAS_AC07 3 DECIMAL
            // PECAS_AC08 3 DECIMAL
            // PECAS_AC09 3 DECIMAL
            // PECAS_AC10 3 DECIMAL
            // PECAS_AC11 3 DECIMAL
            // PECAS_AC12 3 DECIMAL
            // VENDAS_AC01 3 DECIMAL
            // VENDAS_AC02 3 DECIMAL
            // VENDAS_AC03 3 DECIMAL
            // VENDAS_AC04 3 DECIMAL
            // VENDAS_AC05 3 DECIMAL
            // VENDAS_AC06 3 DECIMAL
            // VENDAS_AC07 3 DECIMAL
            // VENDAS_AC08 3 DECIMAL
            // VENDAS_AC09 3 DECIMAL
            // VENDAS_AC10 3 DECIMAL
            // VENDAS_AC11 3 DECIMAL
            // VENDAS_AC12 3 DECIMAL
            // SAZON 12 VARCHAR

            $ektSubdepto['RECORD_NUMBER'] = $i;
            $ektSubdepto['CODIGO'] = $campos[1];
            $ektSubdepto['DESCRICAO'] = $campos[2];
            $ektSubdepto['MARGEM'] = $campos[3];
            $ektSubdepto['PECAS_AC01'] = $campos[4];
            $ektSubdepto['PECAS_AC02'] = $campos[5];
            $ektSubdepto['PECAS_AC03'] = $campos[6];
            $ektSubdepto['PECAS_AC04'] = $campos[7];
            $ektSubdepto['PECAS_AC05'] = $campos[8];
            $ektSubdepto['PECAS_AC06'] = $campos[9];
            $ektSubdepto['PECAS_AC07'] = $campos[10];
            $ektSubdepto['PECAS_AC08'] = $campos[11];
            $ektSubdepto['PECAS_AC09'] = $campos[12];
            $ektSubdepto['PECAS_AC10'] = $campos[13];
            $ektSubdepto['PECAS_AC11'] = $campos[14];
            $ektSubdepto['PECAS_AC12'] = $campos[15];
            $ektSubdepto['VENDAS_AC01'] = $campos[16];
            $ektSubdepto['VENDAS_AC02'] = $campos[17];
            $ektSubdepto['VENDAS_AC03'] = $campos[18];
            $ektSubdepto['VENDAS_AC04'] = $campos[19];
            $ektSubdepto['VENDAS_AC05'] = $campos[20];
            $ektSubdepto['VENDAS_AC06'] = $campos[21];
            $ektSubdepto['VENDAS_AC07'] = $campos[22];
            $ektSubdepto['VENDAS_AC08'] = $campos[23];
            $ektSubdepto['VENDAS_AC09'] = $campos[24];
            $ektSubdepto['VENDAS_AC10'] = $campos[25];
            $ektSubdepto['VENDAS_AC11'] = $campos[26];
            $ektSubdepto['VENDAS_AC12'] = $campos[27];
            $ektSubdepto['SAZON'] = $campos[28];

            $ektSubdepto['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektSubdepto);

            if (!$this->dbekt->insert('ekt_subdepto', $ektSubdepto)) {
                $this->logger->info('Erro ao salvar o ekt_subdepto');
                return;
            } else {
                $this->logger->debug($i . " subdepto(s) inserido(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function importarFornecedores()
    {
        $this->logger->info("IMPORTANDO FORNECEDORES...");

        $this->load->model('ekt/ektfornecedor_model');
        $model = $this->ektfornecedor_model;
        $model->setDb($this->dbekt);

        $this->dbekt->trans_start();

        if (!$model->delete_by_mesano($this->mesano)) {
            $this->logger->info('Erro em deleteByMesAno no importarFornecedores()');
            return;
        }

        // 1 RECORD_NUMBER 4 INTEGER
        // 2 CODIGO 3 DECIMAL
        // 3 RAZAO 12 VARCHAR
        // 4 NOME_FANTASIA 12 VARCHAR
        // 5 CGC 12 VARCHAR
        // 6 INSC 12 VARCHAR
        // 7 DATA_CAD 9 DATE
        // 8 ENDERECO 12 VARCHAR
        // 9 BAIRRO 12 VARCHAR
        // 10 MUNICIPIO 12 VARCHAR
        // 11 UF 12 VARCHAR
        // 12 CEP 12 VARCHAR
        // 13 DDD_FONE 12 VARCHAR
        // 14 FONE 12 VARCHAR
        // 15 DDD_FAX 12 VARCHAR
        // 16 FAX 12 VARCHAR
        // 17 CONTATO 12 VARCHAR
        // 18 NOME_REPRES 12 VARCHAR
        // 19 DDD_REPRES 12 VARCHAR
        // 20 FONE_REPRES 12 VARCHAR
        // 21 COMPRAS_AC 3 DECIMAL
        // 22 DATA_ULT_COMP 9 DATE
        // 23 PECAS_AC 3 DECIMAL
        // 24 TIPO 12 VARCHAR
        // 25 DT_ULTALT 6 DATE

        // abrir arquivo "est_d002.csv"

        // loop linhas

        $linhas = file($this->csvsPath . "est_d002.csv");

        $this->logger->info("Linhas: " . count($linhas));

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", $linha);
            if (count($campos) < 23) {
                $this->logger->debug("A linha deve ter 23 campos. LINHA: [" + $linha + "]");
                return;
            }

            $ektFornecedor = array();

            $ektFornecedor['RECORD_NUMBER'] = $i;
            $ektFornecedor['CODIGO'] = $campos[1];
            $ektFornecedor['RAZAO'] = $campos[2];
            $ektFornecedor['NOME_FANTASIA'] = $campos[3];
            $ektFornecedor['CGC'] = $campos[4];
            $ektFornecedor['INSC'] = $campos[5];
            $ektFornecedor['DATA_CAD'] = $this->datetime_library->dateStrToSqlDate($campos[6]);
            $ektFornecedor['ENDERECO'] = $campos[7];
            $ektFornecedor['BAIRRO'] = $campos[8];
            $ektFornecedor['MUNICIPIO'] = $campos[9];
            $ektFornecedor['UF'] = $campos[10];
            $ektFornecedor['CEP'] = $campos[11];
            $ektFornecedor['DDD_FONE'] = $campos[12];
            $ektFornecedor['FONE'] = $campos[13];
            $ektFornecedor['DDD_FAX'] = $campos[14];
            $ektFornecedor['FAX'] = $campos[15];
            $ektFornecedor['CONTATO'] = $campos[16];
            $ektFornecedor['NOME_REPRES'] = $campos[17];
            $ektFornecedor['DDD_REPRES'] = $campos[18];
            $ektFornecedor['FONE_REPRES'] = $campos[19];
            $ektFornecedor['COMPRAS_AC'] = $campos[20]; // double
            $ektFornecedor['DATA_ULT_COMP'] = $this->datetime_library->dateStrToSqlDate($campos[21]);
            $ektFornecedor['PECAS_AC'] = $campos[22]; // double
            $ektFornecedor['TIPO'] = $campos[23];
            $ektFornecedor['mesano'] = $this->mesano;

            $setembro2015 = DateTime::createFromFormat('d/m/Y', '30/09/2015');
            $outubro2015 = DateTime::createFromFormat('d/m/Y', '31/10/2015');
            $dezembro2015 = DateTime::createFromFormat('d/m/Y', '31/12/2015');

            if ($this->dtMesAno->getTimestamp() > $dezembro2015->getTimestamp()) {
                $ektFornecedor['DT_ULTALT'] = $campos[24]; // date
            }

            $this->handleIudtUserInfo($ektFornecedor);

            $this->logger->debug("Inserindo...");

            if (!$this->dbekt->insert('ekt_fornecedor', $ektFornecedor)) {
                $this->logger->info("Erro ao inserir o fornecedor. CODIGO = [" . $ektFornecedor['CODIGO'] . "]");
                return;
            } else {
                $this->logger->debug($i . " fornecedor(es) inserido(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function importarProdutos()
    {
        $this->dbekt->trans_start();

        $this->logger->info(PHP_EOL . PHP_EOL . "IMPORTANDO PRODUTOS...");

        $this->load->model('ekt/ektproduto_model');
        $model = $this->ektproduto_model;
        $model->setDb($this->dbekt);

        if (!$model->delete_by_mesano($this->mesano)) {
            $this->logger->info('Erro em deleteByMesAno no importarProdutos()');
            return;
        }

        $linhas = file($this->csvsPath . "est_d006.csv");

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", $linha);

            $ektProduto = array();

            $ektProduto['RECORD_NUMBER'] = $i;
            $ektProduto['OVL_PROD'] = $campos[1];
            $ektProduto['FORNEC'] = $campos[2];
            $ektProduto['REFERENCIA'] = $campos[3];
            $ektProduto['GRADE'] = $campos[4];
            $ektProduto['DEPTO'] = $campos[5];
            $ektProduto['SUBDEPTO'] = $campos[6];
            $ektProduto['REDUZIDO'] = $campos[7];
            $ektProduto['DESCRICAO'] = $campos[8];
            $ektProduto['DATA_PCUSTO'] = $this->datetime_library->dateStrToSqlDate($campos[9]);
            $ektProduto['PCUSTO'] = $campos[10];
            $ektProduto['DATA_PVENDA'] = $this->datetime_library->dateStrToSqlDate($campos[11]);
            $ektProduto['PVISTA'] = $campos[12];
            $ektProduto['PPRAZO'] = $campos[13];
            $ektProduto['PPROMO'] = $campos[14];
            $ektProduto['DATA_ULT_VENDA'] = $this->datetime_library->dateStrToSqlDate($campos[15]);
            $ektProduto['PRAZO'] = $campos[16];
            $ektProduto['MARGEM'] = $campos[17];
            $ektProduto['MARGEMC'] = $campos[18];
            $ektProduto['COEF'] = $campos[19];

            $ektProduto['QT01'] = $campos[20];
            $ektProduto['QT02'] = $campos[21];
            $ektProduto['QT03'] = $campos[22];
            $ektProduto['QT04'] = $campos[23];
            $ektProduto['QT05'] = $campos[24];
            $ektProduto['QT06'] = $campos[25];
            $ektProduto['QT07'] = $campos[26];
            $ektProduto['QT08'] = $campos[27];
            $ektProduto['QT09'] = $campos[28];
            $ektProduto['QT10'] = $campos[29];
            $ektProduto['QT11'] = $campos[30];
            $ektProduto['QT12'] = $campos[31];
            $ektProduto['QT13'] = $campos[32];
            $ektProduto['QT14'] = $campos[33];
            $ektProduto['QT15'] = $campos[34];

            $ektProduto['AC01'] = $campos[35];
            $ektProduto['AC02'] = $campos[36];
            $ektProduto['AC03'] = $campos[37];
            $ektProduto['AC04'] = $campos[38];
            $ektProduto['AC05'] = $campos[39];
            $ektProduto['AC06'] = $campos[40];
            $ektProduto['AC07'] = $campos[41];
            $ektProduto['AC08'] = $campos[42];
            $ektProduto['AC09'] = $campos[43];
            $ektProduto['AC10'] = $campos[44];
            $ektProduto['AC11'] = $campos[45];
            $ektProduto['AC12'] = $campos[46];

            $ektProduto['STATUS'] = $campos[47];
            $ektProduto['UNIDADE'] = $campos[48];
            $ektProduto['DATA_CAD'] = $this->datetime_library->dateStrToSqlDate($campos[49]);
            $ektProduto['MODELO'] = $campos[50];
            $ektProduto['QTDE_MES'] = $campos[51];

            $ektProduto['F1'] = $campos[52];
            $ektProduto['F2'] = $campos[53];
            $ektProduto['F3'] = $campos[54];
            $ektProduto['F4'] = $campos[55];
            $ektProduto['F5'] = $campos[56];
            $ektProduto['F6'] = $campos[57];
            $ektProduto['F7'] = $campos[58];
            $ektProduto['F8'] = $campos[59];
            $ektProduto['F9'] = $campos[60];
            $ektProduto['F10'] = $campos[61];
            $ektProduto['F11'] = $campos[62];
            $ektProduto['F12'] = $campos[63];

            $ektProduto['ULT_VENDER'] = $campos[64];

            $setembro2015 = DateTime::createFromFormat('d/m/Y', '30/09/2015');
            $outubro2015 = DateTime::createFromFormat('d/m/Y', '31/10/2015');
            $dezembro2015 = DateTime::createFromFormat('d/m/Y', '31/12/2015');

            if ($this->dtMesAno->getTimestamp() > $setembro2015->getTimestamp()) {
                $ektProduto['ICMS'] = $campos[65];
                $ektProduto['NCM'] = $campos[66];
            }

            if ($this->dtMesAno->getTimestamp() > $outubro2015->getTimestamp()) {
                $ektProduto['FRACIONADO'] = $campos[67];
                $ektProduto['CST'] = $campos[68];
                $ektProduto['TIPO_TRIB'] = $campos[69];
            }

            if ($this->dtMesAno->getTimestamp() > $dezembro2015->getTimestamp()) {
                $ektProduto['DT_ULTALT'] = $this->datetime_library->dateStrToSqlDate($campos[49]);
            }

            $ektProduto['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektProduto);

            if (!$this->dbekt->insert('ekt_produto', $ektProduto)) {
                $this->logger->info('Erro ao salvar o ektProduto. REDUZIDO: [' . $ektProduto['REDUZIDO'] . "]");
                return;
            } else {
                $this->logger->debug($i . " produto(s) inserido(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function importarVendedores()
    {
        $this->dbekt->trans_start();

        $this->logger->info(PHP_EOL . PHP_EOL . "IMPORTANDO VENDEDORES...");

        $model = new \CIBases\Models\DAO\Base\Base_model('ekt_vendedor', 'ekt');

        if (!$this->dbekt->query("DELETE FROM ekt_vendedor WHERE mesano = '" . $this->mesano . "'")) {
            $this->logger->info("Erro ao deletar vendedores do mesano.");
            return;
        }

        $linhas = file($this->csvsPath . "est_d008.csv");

        if (!$linhas or count($linhas) < 0) {
            $this->logger->info("importarVendedores() - Sem dados para importar");
            return;
        }

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", $linha);

            $ektVendedor = array();
            $ektVendedor['RECORD_NUMBER'] = $i;
            $ektVendedor['CODIGO'] = $campos[1];
            $ektVendedor['DESCRICAO'] = $campos[2];
            $ektVendedor['COMIS_VIS'] = $campos[3];
            $ektVendedor['COMIS_PRA'] = $campos[4];
            $ektVendedor['FLAG_GER'] = $campos[5];
            $ektVendedor['SENHA'] = $campos[6];

            $ektVendedor['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektVendedor);

            if (!$this->dbekt->insert('ekt_vendedor', $ektVendedor)) {
                $this->logger->info('Erro ao salvar o ektVendedor. CODIGO = [' . $ektVendedor['CODIGO'] . "]");
                return;
            } else {
                $this->logger->debug($i . " vendedor(es) inserido(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function importarVendas()
    {
        $this->dbekt->trans_start();

        $this->logger->info(PHP_EOL . PHP_EOL . "IMPORTANDO VENDAS...");

        $this->load->model('ekt/ektvenda_model');
        $model = $this->ektvenda_model;
        $model->setDb($this->dbekt);

        if (!$model->delete_by_mesano($this->mesano)) {
            $this->logger->info('Erro em deleteByMesAno no importarVendas()');
            return;
        }

        $linhas = file($this->csvsPath . "ven_d060.csv");

        if (!$linhas or count($linhas) < 0) {
            $this->logger->info("importarVendas() - Sem dados para importar");
            return;
        }

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", $linha);
            if (count($campos) < 44) {
                $this->logger->debug("A linha deve ter 44 campos. LINHA: [" + $linha + "]");
                return;
            }

            $ektVenda = array();

            $ektVenda['RECORD_NUMBER'] = $i;
            $ektVenda['NUMERO'] = $campos[1];
            $ektVenda['SERIE'] = $campos[2];
            $ektVenda['EMISSAO'] = $this->datetime_library->dateStrToSqlDate($campos[3]);
            $ektVenda['VENDEDOR'] = $campos[4];
            $ektVenda['COD_PLANO'] = $campos[5];
            $ektVenda['PLANO'] = $campos[6];
            $ektVenda['MENSAGEM'] = $campos[7];
            $ektVenda['HIST_DESC'] = $campos[8];
            $ektVenda['SUB_TOTAL'] = $campos[9];
            $ektVenda['DESC_ACRES'] = $campos[10];
            $ektVenda['DESC_ESPECIAL'] = $campos[11];
            $ektVenda['TOTAL'] = $campos[12];
            $ektVenda['NOME_CLIENTE'] = $campos[13];
            $ektVenda['COND_PAG'] = $campos[14];
            $ektVenda['FLAG_DV'] = $campos[15];
            $ektVenda['EMITIDO'] = $campos[16];
            $ektVenda['V1'] = $this->datetime_library->dateStrToSqlDate($campos[17], '');
            $ektVenda['V2'] = $this->datetime_library->dateStrToSqlDate($campos[18], '');
            $ektVenda['V3'] = $this->datetime_library->dateStrToSqlDate($campos[19], '');
            $ektVenda['V4'] = $this->datetime_library->dateStrToSqlDate($campos[20], '');
            $ektVenda['V5'] = $this->datetime_library->dateStrToSqlDate($campos[21], '');
            $ektVenda['V6'] = $this->datetime_library->dateStrToSqlDate($campos[22], '');
            $ektVenda['V7'] = $this->datetime_library->dateStrToSqlDate($campos[23], '');
            $ektVenda['V8'] = $this->datetime_library->dateStrToSqlDate($campos[32], '');
            $ektVenda['V9'] = $this->datetime_library->dateStrToSqlDate($campos[33], '');
            $ektVenda['V10'] = $this->datetime_library->dateStrToSqlDate($campos[34], '');
            $ektVenda['V11'] = $this->datetime_library->dateStrToSqlDate($campos[35], '');
            $ektVenda['V12'] = $this->datetime_library->dateStrToSqlDate($campos[36], '');
            $ektVenda['V13'] = $this->datetime_library->dateStrToSqlDate($campos[37], '');
            $ektVenda['P1'] = $campos[24];
            $ektVenda['P2'] = $campos[25];
            $ektVenda['P3'] = $campos[26];
            $ektVenda['P4'] = $campos[27];
            $ektVenda['P5'] = $campos[28];
            $ektVenda['P6'] = $campos[29];
            $ektVenda['P7'] = $campos[30];
            $ektVenda['P8'] = $campos[38];
            $ektVenda['P9'] = $campos[39];
            $ektVenda['P10'] = $campos[40];
            $ektVenda['P11'] = $campos[41];
            $ektVenda['P12'] = $campos[42];
            $ektVenda['P13'] = $campos[43];

            $ektVenda['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektVenda);

            if (!$this->dbekt->insert('ekt_venda', $ektVenda)) {
                $this->logger->info("Erro ao salvar o ektVenda. PV = [" . $ektVenda['NUMERO'] . "]");
                return;
            } else {
                $this->logger->debug($i . " venda(s) inserida(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function importarVendasItens()
    {
        $this->dbekt->trans_start();

        $this->logger->info(PHP_EOL . PHP_EOL . "IMPORTANDO ITENS DAS VENDAS...");

        $this->load->model('ekt/ektvendaitem_model');
        $model = $this->ektvendaitem_model;
        $model->setDb($this->dbekt);

        if (!$model->delete_by_mesano($this->mesano)) {
            $this->logger->info("Erro em deleteByMesAno no importarVendasItens()");
            return;
        }

        $linhas = file($this->csvsPath . "ven_d061.csv");

        if (!$linhas or count($linhas) < 0) {
            $this->logger->info("importarVendasItens() - Sem dados para importar");
            return;
        }

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", $linha);

            $ektVendaItem = array();

            $ektVendaItem['RECORD_NUMBER'] = $i;
            $ektVendaItem['NUMERO_NF'] = $campos[1];
            $ektVendaItem['SERIE'] = $campos[2];
            $ektVendaItem['TELA'] = $campos[3];
            $ektVendaItem['PRODUTO'] = $campos[4];
            $ektVendaItem['QTDE'] = $campos[5];
            $ektVendaItem['UNIDADE'] = $campos[6];
            $ektVendaItem['DESCRICAO'] = $campos[7];
            $ektVendaItem['TAMANHO'] = $campos[8];
            $ektVendaItem['VLR_UNIT'] = $campos[9];
            $ektVendaItem['VLR_TOTAL'] = $campos[10];
            $ektVendaItem['WIN'] = $campos[11];
            $ektVendaItem['PRECO_CUSTO'] = $campos[12];
            $ektVendaItem['PRECO_VISTA'] = $campos[13];

            $ektVendaItem['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektVendaItem);

            if (!$this->dbekt->insert('ekt_venda_item', $ektVendaItem)) {
                $this->logger->info("Erro ao salvar o ektVendaItem. REDUZIDO = '" . $ektVendaItem['PRODUTO'] . "'");
                return;
            } else {
                $this->logger->debug($i . " item(ns) de venda inserido(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function importarPedidos()
    {
        $this->dbekt->trans_start();

        $this->logger->info(PHP_EOL . PHP_EOL . "IMPORTANDO PEDIDOS...");

        $this->load->model('ekt/ektpedido_model');
        $model = $this->ektpedido_model;
        $model->setDb($this->dbekt);

        if (!$model->truncate_table()) {
            $this->logger->info("Erro em truncate table");
            return;
        }

        $linhas = file($this->csvsPath . "ped_d020.csv");

        if (!$linhas or count($linhas) < 0) {
            $this->logger->debug("importarPedidos() - Sem dados para importar");
            return;
        }

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", str_replace('\n', '', $linha));

            $ektPedido = array();

            // só importa pedidos de 01/01/2014 pra frente
            $dtEmissao = DateTime::createFromFormat('Y-m-d', $this->datetime_library->dateStrToSqlDate($campos[2]));
            $janeiro2014 = DateTime::createFromFormat('d/m/Y', '01/01/2014');
            if ($dtEmissao->getTimestamp() < $janeiro2014->getTimestamp()) {
                continue;
            }

            $pedido = $campos[1];

            $ektPedido['RECORD_NUMBER'] = $i;
            $ektPedido['PEDIDO'] = $pedido;
            $ektPedido['EMISSAO'] = $dtEmissao->format('Y-m-d');
            $ektPedido['FORNEC'] = $campos[3];
            $ektPedido['DD1'] = $campos[4];
            $ektPedido['DD2'] = $campos[5];
            $ektPedido['DD3'] = $campos[6];
            $ektPedido['DD4'] = $campos[7];
            $ektPedido['DD5'] = $campos[8];
            $ektPedido['ENTREGA'] = $this->datetime_library->dateStrToSqlDate($campos[9], '');
            $ektPedido['TOTAL'] = $campos[10];
            $ektPedido['SUB_DEPTO'] = $campos[11];
            $ektPedido['QTDE'] = $campos[12];
            $ektPedido['DESCNF'] = $campos[13];
            $ektPedido['DESCDP'] = $campos[14];
            $ektPedido['MES_ENT'] = $campos[15];
            $ektPedido['ANO_ENT'] = $campos[16];
            $ektPedido['PUNIT'] = $campos[17];
            $ektPedido['PTOTAL'] = $campos[18];
            $ektPedido['PRAZO'] = $campos[19];
            $ektPedido['QTDEBX'] = $campos[20];
            $ektPedido['PTOTALBX'] = $campos[21];

            $ektPedido['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektPedido);

            if (!$this->dbekt->insert('ekt_pedido', $ektPedido)) {
                $this->logger->info("Erro ao salvar o ektPedido. PEDIDO = [" . $ektPedido['PEDIDO'] . "]");
                return;
            } else {
                $this->logger->debug($i . " pedido(s) inserido(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function importarEncomendas()
    {
        $this->dbekt->trans_start();

        $this->logger->info(PHP_EOL . PHP_EOL . "IMPORTANDO ENCOMENDAS...");

        $this->load->model('ekt/ektencomenda_model');
        $model = $this->ektencomenda_model;
        $model->setDb($this->dbekt);

        if (!$model->truncate_table()) {
            $this->logger->info("Erro em truncate table no importarEncomendas()");
            return;
        }

        $linhas = file($this->csvsPath . "ped_d070.csv");

        if (!$linhas or count($linhas) < 0) {
            $this->logger->debug("importarEncomendas() - Sem dados para importar");
            return;
        }

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", str_replace('\n', '', $linha));

            $ektEncomenda = array();

            $numEncomenda = $campos[0];

            $ektEncomenda['RECORD_NUMBER'] = $i;
            $ektEncomenda['NUMERO'] = $numEncomenda;
            $ektEncomenda['SERIE'] = $campos[1];
            $ektEncomenda['EMISSAO'] = $this->datetime_library->dateStrToSqlDate($campos[2], '');
            $ektEncomenda['VENDEDOR'] = $campos[3];
            $ektEncomenda['COD_PLANO'] = $campos[4];
            $ektEncomenda['PLANO'] = $campos[5];
            $ektEncomenda['MENSAGEM'] = $campos[6];
            $ektEncomenda['HIST_DESC'] = $campos[7];
            $ektEncomenda['SUB_TOTAL'] = $campos[8];
            $ektEncomenda['DESC_ACRES'] = $campos[9];
            $ektEncomenda['DESC_ESPECIAL'] = $campos[10];
            $ektEncomenda['TOTAL'] = $campos[11];
            $ektEncomenda['NOME_CLIENTE'] = $campos[12];
            $ektEncomenda['COND_PAG'] = $campos[13];
            $ektEncomenda['FLAG_DV'] = $campos[14];
            $ektEncomenda['EMITIDO'] = $campos[15];
            $ektEncomenda['V1'] = $this->datetime_library->dateStrToSqlDate($campos[16], '');
            $ektEncomenda['V2'] = $this->datetime_library->dateStrToSqlDate($campos[17], '');
            $ektEncomenda['V3'] = $this->datetime_library->dateStrToSqlDate($campos[18], '');
            $ektEncomenda['V4'] = $this->datetime_library->dateStrToSqlDate($campos[19], '');
            $ektEncomenda['V5'] = $this->datetime_library->dateStrToSqlDate($campos[20], '');
            $ektEncomenda['V6'] = $this->datetime_library->dateStrToSqlDate($campos[21], '');
            $ektEncomenda['P1'] = $campos[22];
            $ektEncomenda['P2'] = $campos[23];
            $ektEncomenda['P3'] = $campos[24];
            $ektEncomenda['P4'] = $campos[25];
            $ektEncomenda['P5'] = $campos[26];
            $ektEncomenda['P6'] = $campos[27];
            $ektEncomenda['CLIENTE'] = $campos[28];
            $ektEncomenda['FONE'] = $campos[29];
            $ektEncomenda['PRAZO'] = $campos[30];
            $ektEncomenda['SDO_PAGAR'] = $campos[31];

            $ektEncomenda['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektEncomenda);

            if (!$this->dbekt->insert('ekt_encomenda', $ektEncomenda)) {
                $this->logger->info('Erro ao salvar o ektEncomenda. NUMERO = [' . $ektEncomenda['NUMERO'] . "]");
                return;
            } else {
                $this->logger->debug($i . " encomenda(s) inserida(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function importarEncomendasItens()
    {
        $this->dbekt->trans_start();

        $this->logger->info(PHP_EOL . PHP_EOL . "IMPORTANDO ITENS DE ENCOMENDAS...");

        $this->load->model('ekt/ektencomendaitem_model');
        $model = $this->ektencomendaitem_model;
        $model->setDb($this->dbekt);

        if (!$model->truncate_table()) {
            $this->logger->info("Erro em truncate table importarEncomendasItens()");
            return;
        }

        $linhas = file($this->csvsPath . "ped_d071.csv");

        if (!$linhas or count($linhas) < 0) {
            $this->logger->debug("importarEncomendasItens() - Sem dados para importar");
            return;
        }

        $i = 0;

        $todos = array();

        foreach ($linhas as $linha) {

            $i++;
            $campos = explode(";", str_replace('\n', '', $linha));

            $ektEncomendaItem = array();

            $ektEncomendaItem['RECORD_NUMBER'] = $i;
            $ektEncomendaItem['NUMERO_NF'] = $campos[1];
            $ektEncomendaItem['SERIE'] = $campos[2];
            $ektEncomendaItem['TELA'] = $campos[3];
            $ektEncomendaItem['PRODUTO'] = $campos[4];
            $ektEncomendaItem['QTDE'] = $campos[5];
            $ektEncomendaItem['UNIDADE'] = $campos[6];
            $ektEncomendaItem['DESCRICAO'] = $campos[7];
            $ektEncomendaItem['TAMANHO'] = $campos[8];
            $ektEncomendaItem['VLR_UNIT'] = $campos[9];
            $ektEncomendaItem['VLR_TOTAL'] = $campos[10];
            $ektEncomendaItem['WIN'] = $campos[11];
            $ektEncomendaItem['PRECO_CUSTO'] = $campos[12];
            $ektEncomendaItem['PRECO_VISTA'] = $campos[13];
            $ektEncomendaItem['OBS'] = $campos[14];
            $ektEncomendaItem['FLAG'] = $campos[15];
            $ektEncomendaItem['FORNEC'] = $campos[16];
            $ektEncomendaItem['REFERENCIA'] = $campos[17];
            $ektEncomendaItem['GRADE'] = $campos[18];
            $ektEncomendaItem['DEPTO'] = $campos[19];
            $ektEncomendaItem['SUBDEPTO'] = $campos[20];
            $ektEncomendaItem['EMISSAO'] = $this->datetime_library->dateStrToSqlDate($campos[21], '');
            $ektEncomendaItem['FLAG_INT'] = $campos[22];

            $ektEncomendaItem['mesano'] = $this->mesano;

            $this->handleIudtUserInfo($ektEncomendaItem);

            if (!$this->dbekt->insert('ekt_encomenda_item', $ektEncomendaItem)) {
                $this->logger->info('Erro ao salvar o ektEncomendaItem. PRODUTO = [' . $ektEncomendaItem['PRODUTO'] . "]");
                return;
            } else {
                $this->logger->debug($i . " item(ns) de encomenda inserido(s).");
            }
        }

        $this->logger->info("OK!!!!");

        $this->dbekt->trans_complete();
    }

    /**
     */
    private function handleIudtUserInfo(&$entity)
    {
        $entity['inserted'] = $this->agora->format('Y-m-d H:i:s');
        $entity['updated'] = $this->agora->format('Y-m-d H:i:s');
        $entity['estabelecimento_id'] = 1;
        $entity['user_inserted_id'] = 1;
        $entity['user_updated_id'] = 1;
    }


}
