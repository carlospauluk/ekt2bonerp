<?php

    namespace CIBases\Controllers\Base;

    /**
     * Controller base para Forms simples, que lidam com entidade �nica.
     */
    abstract class Form_controller extends \CI_Controller {

        public $model;
        public $data;
        public $form_file;
        public $form_url;
        public $e;

        public function __construct() {
            parent::__construct();

            $this->set_model();
            $this->e = $this->model->empty_record();
            $this->set_form_file();
            $this->set_form_url();
        }

        
        /**
         * É necessário informar qual classe será o modelo.
         */
        public abstract function set_model();

        /**
         * É necessário informar qual o arquivo que contém o formulário.
         */
        public abstract function set_form_file();

        /**
         * É necessário informar qual a URL pela qual a página do formulário é chamada.
         */
        public abstract function set_form_url();

        /**
         * Chama o model para executar as ações de persistência na base de dados.
         */
        public function save() {
            $this->load->helper('form');
            $this->load->library('form_validation');

            $this->set_validations();

            $id = null;

            // Tenta validar
            if ($this->form_validation->run() === TRUE) {
                try {
                	// Se validou, manda salvar.
                    $id = $this->model->save();
//                     $this->e = $this->model->findby_id($id);
                    $this->session->set_flashdata('form_msg_info', 'Registro salvo com sucesso.');
                } catch (\Exception $e) {
                    $this->session->set_flashdata('form_msg_error', $e->getMessage());
                }
            } else {
                $this->session->set_flashdata('form_msg_error', "Erro de validação");
            }
            // Manda construir o formulário
            $content = $this->build_form($id);

            $json = json_encode(
                array(
                    'content' => $content,
                    'url' => base_url($this->form_url . "/" . $id)
                ));
            echo $json;
        }

        public abstract function form($id = null);

        public function show_form($id = null) {
            echo $this->build_form($id);
        }

        public function build_form($id = null) {
            if ($id) {
                $e = $this->model->findby_id($id);
                if ($e) {
                    $this->e = $e;
                } else {
                    $this->session->set_flashdata('form_msg_error', 'Registro não encontrado.');
                }
            }
            return $this->load->view($this->form_file, $this->e, true);
        }
        
        public function post2e() {
        	$e = array();
        	foreach ($this->input->post(NULL) as $key => $value) {
        		$e[$key] = $value;
        	}
        	return $e;
        }


    }