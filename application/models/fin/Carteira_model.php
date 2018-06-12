<?php

require_once('vendor/carlospauluk/cibases/models/base/Base_model.php');

/**
 * Modelo para a tabela fin_carteira.
 */
class Carteira_model extends Base_model {

    public function __construct() {
        parent::__construct("fin_carteira");
    }
    
    public function load_formatters() {
        $this->formatters['dt_consolidado'] = "datetime2date";
    }


}
