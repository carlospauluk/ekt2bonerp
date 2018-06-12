<?php

require_once('vendor/carlospauluk/cibases/controllers/base/CRUD_controller.php');

/**
 * Controller para a entidade Carteira.
 */
class Carteira extends CRUD_controller {

    public function __construct() {
        parent::__construct();
        $this->load->model('fin/carteira_model');

        $this->model = $this->carteira_model;
        $this->form_url = '/fin/carteira/form/';
        $this->list_url = '/fin/carteira/dolist/';
    }

    public function set_validations() {
        $this->form_validation->set_rules('i_abertas', 'Abertas', 'required');
        $this->form_validation->set_rules('i_caixa', 'Caixa', 'required');
        $this->form_validation->set_rules('i_cheque', 'Cheque', 'required');
        $this->form_validation->set_rules('i_codigo', 'Código', 'required');
        $this->form_validation->set_rules('i_concreta', 'Concreta', 'required');
        $this->form_validation->set_rules('i_descricao', 'Descrição', 'required');        
        $this->form_validation->set_rules('i_dt_consolidade', 'Dt Consolidade', 'required');
        
    }
    
    public function show_list() {
        $this->data['itens'] = $this->model->find_all();
        $this->load->view('templates/header');
        $this->load->view('fin/Carteira_list', $this->data);
        $this->load->view('templates/footer');
    }

    public function show_form() {
        $this->load->view('templates/header');
        $this->load->view('fin/Carteira_form', $this->data);
        $this->load->view('templates/footer');
    }

}
