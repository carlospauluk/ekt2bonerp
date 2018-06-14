<?php

/**
 * Controller base para CRUDs.
 */
abstract class CRUD_controller extends CI_Controller {

    public $model;
    public $form_url;
    public $list_url;
    public $data;

    public function __construct() {
        parent::__construct();
    }

    public function del($id) {
        $ret = $this->model->delete($id);
        if ($ret) {
            $this->session->set_flashdata('form_msg_info', 'Registro deletado com sucesso.');
        } else {
            $this->session->set_flashdata('form_msg_error', 'Não foi possível deletar o registro.');
        }
        redirect(base_url($this->list_url));
    }

    public function dolist() {
        $this->data['itens'] = $this->model->find_all();
        $this->show_list();
    }

    public function form($id = null) {
        $this->load->helper('form');
        $this->load->library('form_validation');

        $this->set_validations();

        if ($this->form_validation->run() === TRUE) {
            try {
                $id = $this->model->save();
                $this->session->set_flashdata('form_msg_info', 'Registro salvo com sucesso.');
                redirect(base_url($this->form_url . $id));
            } catch (Exception $e) {
                $this->session->set_flashdata('form_msg_error', $e->getMessage());
            }
        }

        if ($id) {
            $this->data = $this->model->findby_id($id);
            if ($this->data == null) {
                $this->session->set_flashdata('form_msg_error', 'Registro não encontrado.');
            }
        } else {
            $this->data = $this->model->inputpost2data();
        }

        $this->load_form();
    }

    public abstract function show_form();

    public abstract function show_list();
}
