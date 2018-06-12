<?php

class ProgrFinanc extends CI_Controller {

    public $model;
    public $data;

    public function __construct() {
        parent::__construct();
        $this->load->model('fin/ProgrFinanc_model');
        $this->model = $this->ProgrFinanc_model;
    }

    public function gerar($progr_financ_id, $mesano, $reset = false) {
        $this->model->gerar($progr_financ_id, $mesano, $reset);
        
         redirect('/fin/progrfinanc/view/' . $progr_financ_id . '/' . $mesano);
        
    }

    /**
     * 
     * Exibe a tela da Programação Financeira para determinado mesano
     * 
     * @param type $progr_financ_id
     * @param type $mesano
     */
    public function view($progr_financ_id, $mesano) {
        $this->data = $this->model->view($progr_financ_id, $mesano);
        $this->data['progr_financ_id'] = $progr_financ_id;
        $this->data['mesano'] = $mesano;
        $this->data['mesano_anterior'] = Datetime_utils::inc_mesano($mesano, 'month', -1);
        $this->data['mesano_proximo'] = Datetime_utils::inc_mesano($mesano, 'month', 1);
        
        
        $this->load->view('templates/header');
        $this->load->view('fin/ProgrFinanc', $this->data);
        $this->load->view('templates/footer');
    }

    public function bla() {
        $this->model->bla();
    }

    public function teste() {
        $this->model->teste();
    }

}
