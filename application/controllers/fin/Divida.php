<?php

class Divida extends CI_Controller {

    public $model;
    public $data;

    public function __construct() {
        parent::__construct();
        $this->load->model('fin/Divida_model');
        $this->model = $this->Divida_model;
    }
    
    public function listall($dtbase = null) {
        // Lista todos os CGs em andamento
        $this->data['cgs'] = $this->model->listall_cgs($dtbase);
        
        $this->load->view('templates/header');
        $this->load->view('fin/Divida_list', $this->data);
        $this->load->view('templates/footer');
    }
    
    public function listagrup($carteira = null) {
        $this->data['method'] = "listagrup";
        $this->data['carteira'] = $carteira;
        
        $this->data['itens'] = $this->model->listagrup($carteira);
        
        $this->data['total'] = $this->model->totalizar($carteira);
        
        
        $this->load->view('templates/header');
        $this->load->view('fin/DDPCG_list', $this->data);
        $this->load->view('templates/footer');
    }

}
