<?php

require_once('vendor/carlospauluk/cibases/models/base/Base_model.php');

/**
 * Modelo para a tabela crm_promo_campanha.
 */
class Campanha_model extends Base_model {

    public function __construct() {
        parent::__construct("crm_promo_campanha");
    }
    
    public function load_formatters() {
        $this->formatters['dt_inicio'] = "datetime2date";
        $this->formatters['dt_fim'] = "datetime2date";
    }


}
