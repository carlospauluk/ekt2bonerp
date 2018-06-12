<?php

/**
 * Job que realiza a importação dos fornecedores a partir da tabela ekt_fornecedor.
 */
class ImportarEkt2Espelhos extends CI_Controller
{

    private $agora;

    private $dirLogs;

    private $logFile;

    private $MESANOIMPORT;

    private $path;

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        $this->load->database();
        error_reporting(E_ALL);
        ini_set('display_errors', 1);
        
        $this->agora = new DateTime();
    }

    public function importar($ambiente, $mesano, $args)
    {
        $time_start = microtime(true);
        
        echo "Estamos no ambiente: [" . $ambiente . "]" . PHP_EOL;
        echo "Iniciando a importação para o mês/ano: [" . $mesano . "]" . PHP_EOL;
        
        $this->dirLogs = $ambiente == "P" ? "/home/ocabit/bonerp/logs/" : "D:/home/ocabit/bonerp/logs/";
        echo "dirLogs: [" . $this->dirLogs . "]" . PHP_EOL;
        
        $this->logFile = $this->dirLogs . str_replace(" ", "_", $args) . "-" . $this->agora->format('Y-m-d_H-i-s') . ".txt";
        echo "logFile: [" . $this->logFile . "]" . PHP_EOL;
        
        $this->path = $ambiente == "P" ? "/mnt/10.1.1.100-export/" : "\\\\10.1.1.100\\export\\";
        echo "path: [" . $this->path . "]" . PHP_EOL;
        
        $this->MESANOIMPORT = DateTime::createFromFormat('Ymd', $mesano . "01");
        
        $tiposImportacoes = explode("-", $args);
        
        foreach ($tiposImportacoes as $tipo) {
            if (! in_array($tipo, array(
                'GERAIS',
                'FOR',
                'PROD',
                'PED',
                'VEN',
                'ENC'
            ))) {
                die("Tipo de importação inválido: [" . $tipo . "]");
            }
        }
        
        if (in_array('FOR', $tiposImportacoes)) {
            $this->importarFornecedores();
        }
    }

    public function importarFornecedores()
    {
        $this->db->trans_start();
        
        echo "FORNECEDORES";
        
        // deleteByMesAno
        
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
        
        $linhas = file($this->path . "est_d002.csv");
        
        $i = 0;
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
            $ektFornecedor['DATA_CAD'] = $campos[6]; // tratar date
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
            $ektFornecedor['DATA_ULT_COMP'] = $campos[21]; // date
            $ektFornecedor['PECAS_AC'] = $campos[22]; // double
            $ektFornecedor['TIPO'] = $campos[23];
            $ektFornecedor['mesano'] = $mesano;
            
            $setembro2015 = DateTime::createFromFormat('d/m/Y', '30/09/2015');
            $outubro2015 = DateTime::createFromFormat('d/m/Y', '31/10/2015');
            $dezembro2015 = DateTime::createFromFormat('d/m/Y', '31/12/2015');
            
            if ($MESANOIMPORT->getTimestamp() > $dezembro2015->getTimestamp()) {
                $ektFornecedor['DT_ULTALT'] = $campos[24]; // date
            }
        
        $this->handleIudtUserInfo(&$ektFornecedor);
        }
    }

    private function handleIudtUserInfo($obj)
    {
        $ektFornecedor['inserted'] = new DateTime();
        $ektFornecedor['updated'] = new DateTime();
        $ektFornecedor['estabelecimento_id'] = 1;
        $ektFornecedor['user_inserted_id'] = 1;
        $ektFornecedor['user_updated_id'] = 1;
    }
}