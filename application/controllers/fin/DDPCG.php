<?php

class DDPCG extends CI_Controller {

    public $model;
    public $data;

    public function __construct() {
        parent::__construct();
//        $this->output->enable_profiler(TRUE);
        $this->load->model('fin/Divida_model');
        $this->model = $this->Divida_model;
    }

    public function listall($carteira = null) {
        $this->data['method'] = "listall";
        $this->data['carteira'] = $carteira;
        
        $this->data['itens'] = $this->model->listall_ddpcg($carteira);
        
        $this->data['total'] = $this->model->totalizar_ddpcg($carteira);
        
        $this->load->view('templates/header');
        $this->load->view('fin/DDPCG_list', $this->data);
        $this->load->view('templates/footer');
    }
    
    public function listagrup($carteira = null) {
        $this->data['method'] = "listagrup";
        $this->data['carteira'] = $carteira;
        
        $this->data['itens'] = $this->model->listagrup_ddpcg($carteira);
        
        $this->data['total'] = $this->model->totalizar_ddpcg($carteira);
        
        
        $this->load->view('templates/header');
        $this->load->view('fin/DDPCG_list', $this->data);
        $this->load->view('templates/footer');
    }

}
