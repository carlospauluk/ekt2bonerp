<?php

require_once('vendor/carlospauluk/cibases/controllers/base/CRUD_controller.php');

/**
 * Controller para a entidade Campanha.
 */
class Campanha extends CRUD_controller {

    public function __construct() {
        parent::__construct();
        $this->load->model('crm/campanha_model');

        $this->model = $this->campanha_model;
        $this->form_url = '/crm/Campanha/form/';
        $this->list_url = '/crm/Campanha/dolist/';
//        log_message("info", "blablabla");
        // $this->output->enable_profiler(TRUE);
    }

    public function set_validations() {
        $this->form_validation->set_rules('i_descricao', 'Descrição', 'required');
        $this->form_validation->set_rules('i_dt_inicio', 'Dt Início', 'required');
        $this->form_validation->set_rules(
                'i_dt_fim', 'Dt Fim', array(
            'required',
            array(
                'dt_fim_callable',
                function($value) {
                    if ($value) {
                        $this->input->post('i_dt_inicio');
                        $dt_inicio = DateTime::createFromFormat('d/m/Y', $this->input->post('i_dt_inicio'));
                        $dt_fim = DateTime::createFromFormat('d/m/Y', $this->input->post('i_dt_fim'));

                        if ($dt_inicio >= $dt_fim) {
                            return false;
                        } else {
                            return true;
                        }
                    } else {
                        return true;
                    }
                }
            )
                ), array('dt_fim_callable' => 'O campo Dt Fim deve ser maior ou igual a Dt Início')
        );
    }
    
    public function load_list() {
        $this->load->view('templates/header');
        $this->load->view('crm/Campanha_list', $this->data);
        $this->load->view('templates/footer');
    }

    public function load_form() {
        $this->load->view('templates/header');
        $this->load->view('crm/Campanha_form', $this->data);
        $this->load->view('templates/footer');
    }

    public function show_form() {
        // TODO: Implement show_form() method.
    }

    public function show_list() {
        // TODO: Implement show_list() method.
    }
    }
