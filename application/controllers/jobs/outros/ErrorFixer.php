<?php

class ErrorFixer extends CI_Controller
{

    public function __construct()
    {
        parent::__construct();
        ini_set('max_execution_time', 0);
        ini_set('memory_limit', '2048M');
        
        $this->dbbonerp = $this->load->database('bonerp', TRUE);
        
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
    public function datasZeradas()
    {
        $r = $this->dbbonerp->query("SELECT distinct table_name, column_name, column_type FROM information_schema.columns WHERE table_schema = 'bonerp' AND table_name NOT LIKE 'vw%' and column_type LIKE '%date%'")->result_array();
        
        foreach ($r as $l) {
            $dtR = $this->dbbonerp->query("SELECT 1 FROM " . $l['table_name'] . " WHERE DATE_FORMAT(" . $l['column_name'] . ",'%Y-%m-%d') = '0000-00-00'")->result_array();
            if (count($dtR) > 0) {
                echo "Erro na tabela '" . $l['table_name'] . "', campo '" . $l['column_name'] . "'" . PHP_EOL;
            }
        }
        
        
    }
    
   
}
    