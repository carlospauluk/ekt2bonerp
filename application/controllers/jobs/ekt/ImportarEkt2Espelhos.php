<?php

/**
 * Job que realiza a importação dos dados nos CSVs gerados pelo EKT para as tabelas espelho (ekt_*).
 */
class ImportarEkt2Espelhos extends CI_Controller
{

    private $agora;

    /**
     * Qual a pasta dos CSVs. Será obtido a partir da variável de ambiente EKT_CSVS_PATH.
     * @var string
     */
    private $csvsPath;

    /**
     * Qual a pasta do log. Será obtido a partir da variável de ambiente EKT_LOG_PATH.
     * @var string
     */
    private $logPath;

    /**
     * Passado pela linha de comando no formato YYYYMM.
     * @var string
     */
    private $mesAno;

    /**
     * $mesAno convertido para DateTime.
     * @var DateTime
     */
    private $dtMesAno;

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
    }

    /**
     * Método principal
     *
     * $mesano (deve ser passado no formato YYYYMM).
     * $importadores (GERAIS,FOR,PROD,PED,VEN,ENC).
     * 
     * Pela linha de comando, chamar com:
     * 
     * export EKT_CSVS_PATH=/mnt/10.1.1.100-export/
     * export EKT_LOG_PATH=~/dev/github/ekt2bonerp/log/
     * 
     * 
     * php index.php jobs/ekt/ImportarEkt2Espelhos importar YYYYMM FOR-PROD-...
     */
    public function importar($mesano, $importadores)
    {
        $time_start = microtime(true);
        
        echo PHP_EOL . PHP_EOL;
        
        $this->csvsPath = getenv('EKT_CSVS_PATH') or die("EKT_CSVS_PATH não informado\n\n\n");
        $this->logPath = getenv('EKT_LOG_PATH') or die("EKT_LOG_PATH não informado\n\n\n");
        
        echo "csvsPath: [" . $this->csvsPath . "]" . PHP_EOL;
        echo "logPath: [" . $this->logPath . "]" . PHP_EOL;
        $this->logFile = $this->logPath . str_replace(" ", "_", $importadores) . "-" . $this->agora->format('Y-m-d_H-i-s') . ".txt";
        echo "logFile: [" . $this->logFile . "]" . PHP_EOL;
        
        echo "Iniciando a importação para o mês/ano: [" . $mesano . "]" . PHP_EOL;
        
        $this->mesAno = $mesano;
        $this->dtMesAno = DateTime::createFromFormat('Ymd', $mesano . "01");
        if (! $this->dtMesAno instanceof DateTime) {
            die("mesano inválido.\n\n\n");
        }
        
        $tiposImportacoes = explode("-", $importadores);
        
        foreach ($tiposImportacoes as $tipo) {
            if (! in_array($tipo, array(
                'GERAIS',
                'FOR',
                'PROD',
                'PED',
                'VEN',
                'ENC'
            ))) {
                echo "Tipo de importação inválido: [" . $tipo . "]";
            }
        }
        
        if (in_array('FOR', $tiposImportacoes)) {
            $this->importarFornecedores();
        }
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s\n\n";
    }

    public function importarFornecedores()
    {
        $this->db->trans_start();
        
        echo "FORNECEDORES";
        
        $this->load->model('ekt/ektfornecedor_model');
        $model = $this->ektfornecedor_model;
        
        if (! $model->delete_by_mesano($this->mesAno)) {
            log_message('error', 'Erro em deleteByMesAno');
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
        
        $i = 0;
        
        $todos = array();
        
        foreach ($linhas as $linha) {
            
            $i ++;
            $campos = explode(";", $linha);
            if (count($campos) < 23) {
                die("A linha deve ter 23 campos. LINHA: [" + $linha + "]");
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
            $ektFornecedor['mesano'] = $this->mesAno;
            
            $setembro2015 = DateTime::createFromFormat('d/m/Y', '30/09/2015');
            $outubro2015 = DateTime::createFromFormat('d/m/Y', '31/10/2015');
            $dezembro2015 = DateTime::createFromFormat('d/m/Y', '31/12/2015');
            
            if ($this->dtMesAno->getTimestamp() > $dezembro2015->getTimestamp()) {
                $ektFornecedor['DT_ULTALT'] = $campos[24]; // date
            }
            
            $this->handleIudtUserInfo($ektFornecedor);
            
            if (! $this->db->insert('ekt_fornecedor', $ektFornecedor)) {
                log_message('error', 'Erro ao salvar o ektFornecedor');
                return;
            }
        }
        
        $this->db->trans_complete();
    }

    private function handleIudtUserInfo(&$entity)
    {
        $entity['inserted'] = $this->agora->format('Y-m-d H:i:s');
        $entity['updated'] = $this->agora->format('Y-m-d H:i:s');
        $entity['estabelecimento_id'] = 1;
        $entity['user_inserted_id'] = 1;
        $entity['user_updated_id'] = 1;
    }
    
    
}