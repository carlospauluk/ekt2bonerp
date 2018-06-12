<?php

class Vendas_results extends CI_Controller {

    public function __construct() {
        parent::__construct();
        $this->load->model('ven/Vendas_result_model');
        $this->model = $this->Vendas_result_model;
    }

    public function total_por_deptos($desde, $ate) {
        $this->data = $this->model->total_por_deptos($desde, $ate);
               
        $this->load->view('templates/header');
        $this->load->view('ven/Vendas_results_total_por_deptos', $this->data);
        $this->load->view('templates/footer');
    }
    
}
