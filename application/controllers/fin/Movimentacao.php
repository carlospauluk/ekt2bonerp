<?php

class Movimentacao extends CI_Controller {

    public $model;
    public $form_url;
    public $list_url;
    public $data;

    public function __construct() {
        parent::__construct();
        $this->load->model('fin/movimentacao_model');

        $this->model = $this->movimentacao_model;
    }

    public function find() {
        $this->data['itens'] = $this->model->find_apagar('12-2017');
        $this->load->view('templates/header');
        $this->load->view('financeiro/Movimentacao_list', $this->data);
        $this->load->view('templates/footer');
    }
    
    public function listby($mesano, $codigo) {
        $this->data['itens'] = $this->model->listby($mesano, $codigo);
        $this->load->view('templates/header');
        $this->load->view('fin/Movimentacao_list', $this->data);
        $this->load->view('templates/footer');
    }

}
