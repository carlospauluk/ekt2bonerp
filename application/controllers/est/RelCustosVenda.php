<?php

class RelCustosVenda extends CI_Controller {

    public function __construct() {
        parent::__construct();
        $this->load->model('est/Custos_venda_model');
        $this->model = $this->Custos_venda_model;
    }
    
    public function por_fornecedor($mesano_ini, $mesano_fim) {
        $this->data['data'] = $this->model->por_fornecedor($mesano_ini, $mesano_fim);
               
        $this->load->view('templates/header');
        $this->load->view('est/Rel_custos_venda', $this->data);
        $this->load->view('templates/footer');
    }

}
