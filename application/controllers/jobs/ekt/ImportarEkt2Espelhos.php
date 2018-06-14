<?php

/**
 * Job que realiza a importação dos dados nos CSVs gerados pelo EKT para as tabelas espelho (ekt_*).
 */
class ImportarEkt2Espelhos extends CI_Controller
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
     * $mesAno convertido para DateTime.
     *
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
     * export ou set EKT_CSVS_PATH=/mnt/10.1.1.100-export/
     * export ou set EKT_LOG_PATH=~/dev/github/ekt2bonerp/log/
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
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start);
        echo "\n\n\n\n----------------------------------\nTotal Execution Time: " . $execution_time . "s\n\n";
    }

    public function importarFornecedores()
    {
        $this->db->trans_start();
        
        echo "IMPORTANDO FORNECEDORES..." . PHP_EOL . PHP_EOL;
        
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
            } else {
                echo $i . " fornecedor(es) inserido(s)." . PHP_EOL;
            }
        }
        
        $this->db->trans_complete();
    }

    public function importarProdutos()
    {
        $this->db->trans_start();
        
        echo PHP_EOL . PHP_EOL . "IMPORTANDO PRODUTOS..." . PHP_EOL . PHP_EOL;
        
        $this->load->model('ekt/ektproduto_model');
        $model = $this->ektproduto_model;
        
        if (! $model->delete_by_mesano($this->mesAno)) {
            log_message('error', 'Erro em deleteByMesAno');
            return;
        }
        
        $linhas = file($this->csvsPath . "est_d006.csv");
        
        $i = 0;
        
        $todos = array();
        
        foreach ($linhas as $linha) {
            
            $i ++;
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
            
            $ektProduto['mesano'] = $this->mesAno;
            
            $this->handleIudtUserInfo($ektProduto);
            
            if (! $this->db->insert('ekt_produto', $ektProduto)) {
                log_message('error', 'Erro ao salvar o ektProduto');
                return;
            } else {
                echo $i . " produto(s) inserido(s)." . PHP_EOL;
            }
        }
        
        $this->db->trans_complete();
    }

    public function importarVendedores()
    {
        $this->db->trans_start();
        
        echo PHP_EOL . PHP_EOL . "IMPORTANDO VENDEDORES..." . PHP_EOL . PHP_EOL;
        
        $this->load->model('ekt/ektvendedor_model');
        $model = $this->ektvendedor_model;
        
        if (! $model->truncate_table()) {
            log_message('error', 'Erro em truncate table');
            return;
        }
        
        $linhas = file($this->csvsPath . "est_d008.csv");
        
        if (! $linhas or count($linhas) < 0) {
            die("importarVendedores() - Sem dados para importar");
        }
        
        $i = 0;
        
        $todos = array();
        
        foreach ($linhas as $linha) {
            
            $i ++;
            $campos = explode(";", $linha);
            
            $ektVendedor = array();
            $ektVendedor['RECORD_NUMBER'] = $campos[1];
            $ektVendedor['CODIGO'] = $campos[2];
            $ektVendedor['COMIS_VIS'] = $campos[3];
            $ektVendedor['COMIS_PRA'] = $campos[4];
            $ektVendedor['FLAG_GER'] = $campos[5];
            $ektVendedor['SENHA'] = $campos[6];
            
            $ektVendedor['mesano'] = $this->mesAno;
            
            $this->handleIudtUserInfo($ektVendedor);
            
            if (! $this->db->insert('ekt_vendedor', $ektVendedor)) {
                log_message('error', 'Erro ao salvar o ektVendedor');
                return;
            } else {
                echo $i . " vendedor(es) inserido(s)." . PHP_EOL;
            }
        }
        
        $this->db->trans_complete();
    }

    public function importarVendas()
    {
        $this->db->trans_start();
        
        echo PHP_EOL . PHP_EOL . "IMPORTANDO VENDAS..." . PHP_EOL . PHP_EOL;
        
        $this->load->model('ekt/ektvenda_model');
        $model = $this->ektvenda_model;
        
        if (! $model->delete_by_mesano($this->mesAno)) {
            log_message('error', 'Erro em deleteByMesAno');
            return;
        }
        
        $linhas = file($this->csvsPath . "ven_d060.csv");
        
        if (! $linhas or count($linhas) < 0) {
            die("importarVendas() - Sem dados para importar");
        }
        
        $i = 0;
        
        $todos = array();
        
        foreach ($linhas as $linha) {
            
            $i ++;
            $campos = explode(";", $linha);
            if (count($campos) < 44) {
                die("A linha deve ter 44 campos. LINHA: [" + $linha + "]");
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
            
            $ektVenda['mesano'] = $this->mesAno;
            
            $this->handleIudtUserInfo($ektVenda);
            
            if (! $this->db->insert('ekt_venda', $ektVenda)) {
                log_message('error', 'Erro ao salvar o ektVenda');
                return;
            } else {
                echo $i . " venda(s) inserida(s)." . PHP_EOL;
            }
        }
        
        $this->db->trans_complete();
    }

    public function importarVendasItens()
    {
        $this->db->trans_start();
        
        echo PHP_EOL . PHP_EOL . "IMPORTANDO ITENS DAS VENDAS..." . PHP_EOL . PHP_EOL;
        
        $this->load->model('ekt/ektvendaitem_model');
        $model = $this->ektvendaitem_model;
        
        if (! $model->delete_by_mesano($this->mesAno)) {
            log_message('error', 'Erro em deleteByMesAno');
            return;
        }
        
        $linhas = file($this->csvsPath . "ven_d061.csv");
        
        if (! $linhas or count($linhas) < 0) {
            die("importarVendasItens() - Sem dados para importar");
        }
        
        $i = 0;
        
        $todos = array();
        
        foreach ($linhas as $linha) {
            
            $i ++;
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
            
            $ektVendaItem['mesano'] = $this->mesAno;
            
            $this->handleIudtUserInfo($ektVendaItem);
            
            if (! $this->db->insert('ekt_venda_item', $ektVendaItem)) {
                log_message('error', 'Erro ao salvar o ektVendaItem');
                return;
            } else {
                echo $i . " item(ns) de venda inserido(s)." . PHP_EOL;
            }
        }
        
        $this->db->trans_complete();
    }

    public function importarPedidos()
    {
        $this->db->trans_start();
        
        echo PHP_EOL . PHP_EOL . "IMPORTANDO PEDIDOS..." . PHP_EOL . PHP_EOL;
        
        $this->load->model('ekt/ektpedido_model');
        $model = $this->ektpedido_model;
        
        if (! $model->truncate_table()) {
            log_message('error', 'Erro em truncate table');
            return;
        }
        
        $linhas = file($this->csvsPath . "ped_d020.csv");
        
        if (! $linhas or count($linhas) < 0) {
            die("importarPedidos() - Sem dados para importar");
        }
        
        $i = 0;
        
        $todos = array();
        
        foreach ($linhas as $linha) {
            
            $i ++;
            $campos = explode(";", str_replace('\n', '', $linha));
            
            $ektPedido = array();
            
            // só importa pedidos de 01/01/2014 pra frente
            $dtEmissao = DateTime::createFromFormat('Y-m-d', $this->datetime_library->dateStrToSqlDate($campos[2]));
            $janeiro2014 = DateTime::createFromFormat('d/m/Y', '01/01/2014');
            if ($dtEmissao->getTimestamp() < $janeiro2014->getTimestamp()) {
                continue;
            }
            
            $pedido = $campos[1];
            
            // Verifica se já existe para não reimportar.
//             $jaExiste = $model->findby_pedido($pedido);
//             if ($jaExiste) {
//                 echo "PEDIDO $pedido já existente na base." . PHP_EOL;
//                 continue;
//             }
            
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
            
            $ektPedido['mesano'] = $this->mesAno;
            
            $this->handleIudtUserInfo($ektPedido);
            
            if (! $this->db->insert('ekt_pedido', $ektPedido)) {
                log_message('error', 'Erro ao salvar o ektPedido');
                return;
            } else {
                echo $i . " pedido(s) inserido(s)." . PHP_EOL;
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